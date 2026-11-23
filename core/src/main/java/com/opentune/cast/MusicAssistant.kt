package com.opentune.cast

import android.os.SystemClock
import com.opentune.data.DebugLog as Log
import com.opentune.data.Http
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.URI
import java.net.URLDecoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.roundToInt
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import org.json.JSONObject

/** Something a Music Assistant server said on its own: a player or queue that changed. */
data class MaEvent(val event: String, val objectId: String?, val data: Any?)

/** A command the server turned down, with its error code (20 and up: sign-in problems). */
class MaException(val code: Int, message: String) : Exception(message) {
    /** The token is missing, wrong or revoked: trying again with it never helps. */
    val signIn: Boolean get() = code in 20..23
}

/** What [MusicAssistant] needs of a server; a recording stands in for it in tests. */
interface MaApi {
    val events: SharedFlow<MaEvent>
    /** Sends [command] and waits for its result: a JSONObject, a JSONArray, a plain value or null. */
    suspend fun call(command: String, args: JSONObject = JSONObject()): Any?
    /**
     * The YouTube Music provider the server has, as named in a song's address (`<it>://track/<id>`):
     * the official one (`ytmusic`) or another, such as a free one (`ytmusic_free`); null for none.
     */
    suspend fun youTubeMusicProvider(): String? = null
    suspend fun hasYouTubeMusic(): Boolean = youTubeMusicProvider() != null
    /** Done with it, for one user of it. */
    fun release() = Unit
}

/** What a server says first. */
data class MaServerInfo(val serverId: String, val version: String, val schema: Int, val minSchema: Int) {
    companion object {
        fun from(json: JSONObject) = MaServerInfo(
            json.optString("server_id"), json.optString("server_version"),
            json.optInt("schema_version"), json.optInt("min_supported_schema_version"),
        )
    }
}

/**
 * One Music Assistant server, over its WebSocket API at `ws://<host>:8095/ws`.
 * The server speaks first with its [MaServerInfo]; from schema 28 on (MA 2.7)
 * nothing else is answered until an `auth` command with a token has been
 * accepted. Replies come back with the `message_id` they answer; a long list
 * comes in `partial` pieces. Everything else is an event.
 *
 * A dropped connection is opened again with a doubling wait while anything
 * still uses it, except after the token was refused: then it stays shut, and
 * says why, until the token is changed.
 */
class MaClient(
    val url: String,
    private val token: () -> String?,
    private val http: OkHttpClient = localClient,
) : MaApi {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val ids = AtomicInteger(1)
    private val waiting = ConcurrentHashMap<String, CompletableDeferred<Any?>>()
    private val pieces = ConcurrentHashMap<String, JSONArray>()
    private val opening = Mutex()
    @Volatile private var socket: WebSocket? = null
    @Volatile private var ready: CompletableDeferred<MaServerInfo>? = null
    @Volatile private var users = 0
    @Volatile private var retry: Job? = null
    @Volatile private var providers: String? = null
    @Volatile private var providersRead = false
    /** The token the server turned down, or the reason it can't be used at all; no retrying until it changes. */
    @Volatile private var refusedToken: String? = null
    @Volatile private var refusal: MaException? = null

    private val _events = MutableSharedFlow<MaEvent>(extraBufferCapacity = 64)
    override val events: SharedFlow<MaEvent> = _events

    private val _problem = MutableStateFlow<String?>(null)
    /** Why the server can't be used, when it can't. */
    val problem: StateFlow<String?> = _problem

    /** The server's own report, once connected. */
    @Volatile var info: MaServerInfo? = null
        private set

    /** The phone's address on the server's network, for it to fetch from the phone. */
    val localAddress: InetAddress? by lazy {
        runCatching {
            val uri = URI(url)
            DatagramSocket().use { it.connect(InetAddress.getByName(uri.host), if (uri.port > 0) uri.port else PORT); it.localAddress }
        }.getOrNull()
    }

    /** Closes the connection a while after its last user lets it go. */
    private var closing: Job? = null

    fun retain() = synchronized(this) {
        users++
        closing?.cancel()
        closing = null
    }

    /**
     * Done with it, for one user. The last user's release leaves the connection open for
     * [LINGER_MS]: the scan lets it go seconds before a speaker is picked, and opening it
     * again meant connecting and signing in twice.
     */
    override fun release() = synchronized(this) {
        users = (users - 1).coerceAtLeast(0)
        if (users > 0) return@synchronized
        closing?.cancel()
        closing = scope.launch {
            delay(LINGER_MS)
            synchronized(this@MaClient) { if (users == 0) shut() }
        }
    }

    /** Closes the connection now, whoever is using it (a one-off client, like Test connection's). */
    fun closeNow() = synchronized(this) {
        closing?.cancel()
        closing = null
        shut()
    }

    /** Opens the connection (signing in) if it isn't, and returns what the server said. */
    suspend fun connect(): MaServerInfo = opening.withLock {
        refusal?.let { if (it.code == UNSUPPORTED || refusedToken == token()?.trim()) throw it }
        ready?.takeIf { it.isCompleted && !it.isCancelled && socket != null }?.let { return@withLock it.await() }
        val handshake = CompletableDeferred<MaServerInfo>()
        ready = handshake
        val request = Request.Builder().url(url).build()
        socket = http.newWebSocket(request, Listener(handshake))
        try {
            withTimeout(CONNECT_MS) { handshake.await() }
        } catch (e: Exception) {
            socket?.cancel()
            socket = null
            throw e
        }
    }

    override suspend fun call(command: String, args: JSONObject): Any? {
        val ws = connect().let { socket } ?: throw MaException(0, "Not connected")
        return send(ws, command, args)
    }

    override suspend fun youTubeMusicProvider(): String? {
        if (providersRead) return providers
        val found = runCatching<String?> {
            val list = when (val all = call("providers")) {
                is JSONObject -> all.keys().asSequence().mapNotNull { all.optJSONObject(it) }.toList()
                is JSONArray -> (0 until all.length()).mapNotNull { all.optJSONObject(it) }
                else -> emptyList()
            }
            MusicAssistant.youTubeMusicOf(list)
        }
        // Not taken as known when the server couldn't be asked; asked again next time.
        if (found.isSuccess) {
            providers = found.getOrNull()
            providersRead = true
        }
        return found.getOrNull()
    }

    private suspend fun send(ws: WebSocket, command: String, args: JSONObject): Any? {
        val id = ids.getAndIncrement().toString()
        val reply = CompletableDeferred<Any?>()
        waiting[id] = reply
        try {
            val message = JSONObject().put("message_id", id).put("command", command).put("args", args)
            if (!ws.send(message.toString())) throw MaException(0, "Connection closed")
            return withTimeout(CALL_MS) { reply.await() }
        } finally {
            waiting.remove(id)
            pieces.remove(id)
        }
    }

    private inner class Listener(private val handshake: CompletableDeferred<MaServerInfo>) : WebSocketListener() {
        /** The server's first message has come; everything after it (the `auth` reply too) is a reply or an event. */
        @Volatile private var greeted = false

        override fun onMessage(webSocket: WebSocket, text: String) {
            if (webSocket !== socket) return
            val json = runCatching { JSONObject(text) }.getOrNull() ?: return
            if (!greeted) {
                // The first thing the server says is who it is; or, before setup, that it isn't ready.
                if (json.has("error_code") && !json.has("server_version")) { handshake.completeExceptionally(error(json)); return }
                greeted = true
                scope.launch { greet(webSocket, MaServerInfo.from(json), handshake) }
                return
            }
            when {
                json.has("event") -> _events.tryEmit(MaEvent(json.getString("event"), json.optString("object_id").ifBlank { null }, json.opt("data")))
                json.has("error_code") -> waiting[json.optString("message_id")]?.completeExceptionally(error(json))
                json.has("result") -> {
                    val id = json.optString("message_id")
                    val result = json.opt("result")
                    if (json.optBoolean("partial")) {
                        val got = pieces.getOrPut(id) { JSONArray() }
                        (result as? JSONArray)?.let { for (i in 0 until it.length()) got.put(it.get(i)) }
                    } else {
                        val earlier = pieces.remove(id)
                        val whole = if (earlier != null && result is JSONArray) earlier.also { for (i in 0 until result.length()) it.put(result.get(i)) } else earlier ?: result
                        waiting[id]?.complete(if (whole == JSONObject.NULL) null else whole)
                    }
                }
            }
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            webSocket.close(1000, null)
        }
        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) = lost(webSocket, handshake, MaException(0, "Closed: $reason"))
        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) = lost(webSocket, handshake, t)
    }

    private suspend fun greet(ws: WebSocket, server: MaServerInfo, handshake: CompletableDeferred<MaServerInfo>) {
        try {
            Log.i(TAG, "Music Assistant ${server.version} at $url: schema ${server.schema}, oldest client schema ${server.minSchema}")
            if (server.minSchema > SCHEMA) throw MaException(UNSUPPORTED, "This Music Assistant server (${server.version}) needs a newer OpenTune")
            // A server that says nothing of its schema is let through: signing in tells.
            if (server.schema in 1 until MIN_SCHEMA) throw MaException(UNSUPPORTED, "Music Assistant 2.7 or newer is needed (this server is ${server.version.ifBlank { "older" }})")
            val key = token()?.trim().orEmpty()
            if (key.isEmpty()) throw MaException(20, "Music Assistant needs an access token")
            val ok = send(ws, "auth", JSONObject().put("token", key))
            val accepted = when (ok) {
                is JSONObject -> ok.optBoolean("authenticated", true)
                is Boolean -> ok
                null -> false
                else -> true
            }
            if (!accepted) throw MaException(23, "Music Assistant didn't accept the token")
            info = server
            _problem.value = null
            handshake.complete(server)
            // Anything that follows the server refreshes what it knows now.
            _events.tryEmit(MaEvent(CONNECTED, null, null))
        } catch (e: Exception) {
            if (e is MaException && (e.signIn || e.code == UNSUPPORTED)) {
                // A refused token, or a server this client can't talk to: no retrying until something changes.
                refusedToken = token()?.trim()
                refusal = e
                _problem.value = e.message
            }
            handshake.completeExceptionally(e)
            ws.close(1000, null)
        }
    }

    private fun lost(ws: WebSocket, handshake: CompletableDeferred<MaServerInfo>, cause: Throwable) {
        if (ws !== socket) return
        socket = null
        if (!handshake.isCompleted) handshake.completeExceptionally(cause)
        waiting.values.forEach { it.completeExceptionally(MaException(0, "Connection lost")) }
        if (refusal != null || users == 0 || retry?.isActive == true) return
        Log.w(TAG, "connection to $url lost; trying again")
        _events.tryEmit(MaEvent(DISCONNECTED, null, null))
        retry = scope.launch {
            var wait = FIRST_RETRY_MS
            while (users > 0 && refusal == null) {
                delay(wait)
                val tried = runCatching { connect() }
                if (tried.isSuccess) return@launch
                Log.w(TAG, "reconnect to $url failed: ${tried.exceptionOrNull()?.message}")
                wait = (wait * 2).coerceAtMost(MAX_RETRY_MS)
            }
        }
    }

    private fun shut() {
        retry?.cancel()
        socket?.close(1000, null)
        socket = null
        ready = null
        providers = null
        providersRead = false
    }

    private fun error(json: JSONObject) = MaException(json.optInt("error_code"), json.optString("details").ifBlank { "Music Assistant error ${json.optInt("error_code")}" })

    companion object {
        private const val TAG = "Cast"
        const val PORT = 8095
        /**
         * The API this client speaks, checked against a server's oldest supported client
         * schema. It uses only commands from schema 28 on, which servers still accept
         * (MA's own client does the same with its newer number).
         */
        const val SCHEMA = 65
        /** Tokens, and so `auth`, came in with schema 28 (MA 2.7). */
        const val MIN_SCHEMA = 28
        /** Not sent by the server: the client's own markers for a fresh or a lost connection. */
        const val CONNECTED = "opentune_connected"
        const val DISCONNECTED = "opentune_disconnected"
        private const val CONNECT_MS = 8_000L
        private const val CALL_MS = 10_000L
        /** How long a connection nothing uses is kept open, for the next user. */
        private const val LINGER_MS = 30_000L
        /** Not an MA error code: a server this client can't speak to at all. */
        private const val UNSUPPORTED = -1
        private const val FIRST_RETRY_MS = 1_000L
        private const val MAX_RETRY_MS = 30_000L

        /** Straight to the server, never through a proxy: it's on the home network. */
        private val localClient: OkHttpClient by lazy {
            Http.client.newBuilder().proxy(java.net.Proxy.NO_PROXY).pingInterval(30, TimeUnit.SECONDS).readTimeout(0, TimeUnit.MILLISECONDS).build()
        }

        private val clients = ConcurrentHashMap<String, MaClient>()

        /** The one client for the server at [url], shared by every player on it. */
        fun forServer(url: String, token: () -> String?): MaClient = clients.getOrPut(wsUrl(url)) { MaClient(wsUrl(url), token) }

        /** Drops every connection, after the token or server was changed. */
        fun forgetAll() {
            clients.values.forEach { it.closeNow() }
            clients.clear()
        }

        /** `ws://host:8095/ws` from what someone typed or the server announced: a host, host:port or an http(s) address. */
        fun wsUrl(address: String): String {
            val a = address.trim().trimEnd('/')
            val withScheme = when {
                a.startsWith("ws://") || a.startsWith("wss://") -> a
                a.startsWith("http://") -> "ws://" + a.removePrefix("http://")
                a.startsWith("https://") -> "wss://" + a.removePrefix("https://")
                else -> "ws://$a"
            }
            val uri = URI(withScheme)
            val port = if (uri.port > 0) ":${uri.port}" else if (uri.scheme == "ws") ":$PORT" else ""
            val path = uri.path.orEmpty().removeSuffix("/ws").trimEnd('/')
            return "${uri.scheme}://${uri.host}$port$path/ws"
        }
    }
}

/** One song in a Music Assistant queue. */
data class MaItem(
    val id: String,
    val uri: String?,
    val title: String,
    val artist: String,
    val album: String?,
    val imageUrl: String?,
    val durationMs: Long,
) {
    /** Its YouTube id, for a song from a YouTube Music provider: one the phone can play itself. */
    val youTubeId: String? get() = MusicAssistant.youTubeIdOf(uri)
}

/**
 * A Music Assistant queue as it last said it was. [state] is "playing", "paused" or "idle";
 * the current song was at [positionMs] when the phone's clock (elapsedRealtime) read [at].
 */
data class MaQueue(
    val state: String = "idle",
    val index: Int = -1,
    val itemId: String? = null,
    val positionMs: Long = 0,
    val at: Long = 0,
    val shuffle: Boolean = false,
    val repeat: String = "off",
    val count: Int = 0,
) {
    val playing: Boolean get() = state == "playing"

    fun positionNow(now: Long = SystemClock.elapsedRealtime()): Long =
        if (playing) positionMs + (now - at).coerceAtLeast(0) else positionMs
}

/**
 * A Music Assistant player as a cast target. Its queue is Music Assistant's: the phone's
 * queue is handed over once ([handOff]), and from then on OpenTune shows what the queue says
 * and sends it commands, as Music Assistant's own apps do (the session's player while casting
 * is [MusicAssistantPlayer][com.opentune.playback.MusicAssistantPlayer]). Keeping the queue on
 * the phone as well, and sending the speaker one song at a time, raced it: songs started over,
 * plays and pauses arrived out of order, and the two drifted apart.
 */
class MusicAssistant(
    override val name: String,
    private val queueId: String,
    private val playerId: String,
    private val client: MaApi,
) : Receiver {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val _status = MutableStateFlow(RemoteStatus())
    override val status: StateFlow<RemoteStatus> = _status
    private val _queue = MutableStateFlow(MaQueue())
    /** The queue as it last said it was. */
    val queue: StateFlow<MaQueue> = _queue
    private val _items = MutableStateFlow<List<MaItem>>(emptyList())
    /** The queue's songs, in the order they play. */
    val items: StateFlow<List<MaItem>> = _items
    private var listening: Job? = null
    private var itemsJob: Job? = null

    val localAddress: InetAddress? get() = (client as? MaClient)?.localAddress

    /** Set once MA's YouTube Music has failed to play songs here; songs come through the phone from then on. */
    @Volatile var youTubeMusicFailed = false
        private set

    /** The server's YouTube Music provider, to send a YouTube song to by its id; null to send it through the phone. */
    suspend fun youTubeMusic(): String? = if (youTubeMusicFailed) null else client.youTubeMusicProvider()

    /** Starts following the player's queue. */
    suspend fun attach() = withContext(Dispatchers.IO) {
        // Subscribed before going on, so nothing MA says from here on is missed.
        listening = scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { client.events.collect(::onEvent) }
        refresh()
    }

    /** Reads the queue, its songs and the player as they are now. */
    private suspend fun refresh() {
        runCatching { client.call("player_queues/all") as? JSONArray }.getOrNull()
            ?.let { all -> (0 until all.length()).map { all.getJSONObject(it) }.firstOrNull { it.optString("queue_id") == queueId } }
            ?.let(::onQueue)
        loadItems()
        runCatching { client.call("players/all") as? JSONArray }.getOrNull()
            ?.let { all -> (0 until all.length()).map { all.getJSONObject(it) }.firstOrNull { it.optString("player_id") == playerId } }
            ?.let(::onPlayer)
    }

    private suspend fun loadItems() {
        val all = try {
            client.call("player_queues/items", JSONObject().put("queue_id", queueId).put("limit", ITEMS_MAX).put("offset", 0)) as? JSONArray
        } catch (e: Exception) {
            Log.w(TAG, "couldn't read the queue on $name: ${e.message}")
            return
        } ?: return
        _items.value = (0 until all.length()).mapNotNull { all.optJSONObject(it)?.let(::itemOf) }
        updateStatus()
    }

    /** Reads the songs again in a moment; a hand-off changes them several times in a row. */
    private fun reloadItems() {
        itemsJob?.cancel()
        itemsJob = scope.launch {
            delay(ITEMS_SETTLE_MS)
            loadItems()
        }
    }

    /**
     * Hands [songs] to the speaker as its queue, the first starting at [positionMs]. The first
     * goes alone, so it starts at once; the rest follow in batches. Songs for MA's YouTube Music
     * come through the phone instead if it can't play them ([CastMedia.fallbackUrl]).
     */
    suspend fun handOff(songs: List<CastMedia>, positionMs: Long) = withContext(Dispatchers.IO) {
        val first = songs.firstOrNull() ?: return@withContext
        val before = _queue.value.itemId
        try {
            send(listOf(first), "replace")
        } catch (e: Exception) {
            Log.w(TAG, "$name wouldn't play ${first.title}: ${e.message}")
            if (e is MaException && e.signIn) _status.value = _status.value.copy(state = RemoteState.GONE, at = SystemClock.elapsedRealtime())
            return@withContext
        }
        if (positionMs >= SEEK_MIN_MS) scope.launch { seekWhenStarted(positionMs, before) }
        for (batch in songs.drop(1).chunked(BATCH)) {
            try {
                send(batch, "add")
            } catch (e: Exception) {
                Log.w(TAG, "couldn't queue ${batch.size} songs on $name: ${e.message}")
                if (e is MaException && e.signIn) break
            }
        }
    }

    /** Adds [songs] to the queue: straight after the current song, or at the end. */
    suspend fun add(songs: List<CastMedia>, next: Boolean) = command {
        if (songs.isNotEmpty()) send(songs, if (next) "next" else "add")
    }

    /** `play_media` with [songs]: by YouTube Music's addresses first, then the phone's if MA can't play those. */
    private suspend fun send(songs: List<CastMedia>, option: String) {
        fun args(media: List<Any>) = JSONObject().put("queue_id", queueId).put("media", JSONArray(media)).put("option", option)
        fun address(m: CastMedia): Any = if (m.url.startsWith("http") && !m.live && !addressesOnly) builtinTrack(m) else m.url
        if (!youTubeMusicFailed && songs.any { it.fallbackUrl != null && !it.url.startsWith("http") }) {
            try {
                client.call("player_queues/play_media", args(songs.map(::address)))
                return
            } catch (e: MaException) {
                if (e.signIn) throw e
                // MA's YouTube Music couldn't play them (signed out, or turned away by YouTube): through the phone, then.
                Log.w(TAG, "$name couldn't take ${songs.size} songs from YouTube Music (${e.message}); playing them through the phone")
                youTubeMusicFailed = true
                Cast.say(com.opentune.i18n.Tr.t("Music Assistant's YouTube Music couldn't play this, so songs play through this phone"))
            }
        }
        val viaPhone = songs.map { s -> s.fallbackUrl?.let { s.copy(url = it, fallbackUrl = null) } ?: s }
        try {
            client.call("player_queues/play_media", args(viaPhone.map(::address)))
        } catch (e: MaException) {
            if (e.signIn || addressesOnly || viaPhone.none { it.url.startsWith("http") && !it.live }) throw e
            Log.w(TAG, "$name wouldn't take songs with their details (${e.message}); sending the addresses alone")
            addressesOnly = true
            client.call("player_queues/play_media", args(viaPhone.map { it.url }))
        }
    }

    /** Set once the server has turned down songs sent as whole tracks; addresses alone go from then on. */
    @Volatile private var addressesOnly = false

    /**
     * Moves the song just handed over to [positionMs] once it's under way: MA turns a seek down
     * (its catch-all "invalid command") until then. The place moves on with the time spent waiting.
     */
    private suspend fun seekWhenStarted(positionMs: Long, before: String?) {
        val from = SystemClock.elapsedRealtime()
        var started: String? = null
        repeat(START_SEEK_TRIES) {
            delay(START_SEEK_GAP_MS)
            val now = _queue.value.itemId
            if (now == null || now == before) return@repeat
            // Moved on from the song handed over (skipped meanwhile): its place no longer matters.
            if (started != null && now != started) return
            started = now
            if (seekTo(positionMs + SystemClock.elapsedRealtime() - from)) return
        }
    }

    override suspend fun load(media: CastMedia, positionMs: Long, play: Boolean) {
        handOff(listOf(media), positionMs)
        if (!play) pause()
    }

    override suspend fun play() = command { queue("player_queues/play") }
    override suspend fun pause() = command { queue("player_queues/pause") }
    suspend fun next() = command { queue("player_queues/next") }
    suspend fun previous() = command { queue("player_queues/previous") }

    /** Plays the queue's song at [index], from [positionMs]. */
    suspend fun playIndex(index: Int, positionMs: Long = 0) = command {
        queue("player_queues/play_index", JSONObject().put("index", index).put("seek_position", positionMs / 1000))
    }

    override suspend fun seek(positionMs: Long) = command {
        if (seekTo(positionMs)) _queue.value = _queue.value.copy(positionMs = positionMs, at = SystemClock.elapsedRealtime())
    }

    /** Takes the queue's song [itemId] out. */
    suspend fun remove(itemId: String) = command { queue("player_queues/delete_item", JSONObject().put("item_id_or_index", itemId)) }

    /** Moves the queue's song [itemId] [shift] places (later in the queue for more than 0). */
    suspend fun move(itemId: String, shift: Int) = command {
        queue("player_queues/move_item", JSONObject().put("queue_item_id", itemId).put("pos_shift", shift))
    }

    suspend fun shuffle(on: Boolean) = command { queue("player_queues/shuffle", JSONObject().put("shuffle_enabled", on)) }

    /** [mode]: "off", "one" or "all". */
    suspend fun repeat(mode: String) = command { queue("player_queues/repeat", JSONObject().put("repeat_mode", mode)) }

    override suspend fun setVolume(level: Float) = command {
        val percent = (level.coerceIn(0f, 1f) * 100).roundToInt()
        client.call("players/cmd/volume_set", JSONObject().put("player_id", playerId).put("volume_level", percent))
        _status.value = _status.value.copy(volume = percent / 100f)
    }

    /** Set once the player has turned a seek down as unsupported; none are sent after. */
    @Volatile private var cantSeek = false

    /**
     * Moves the song to [positionMs]; false if MA turned it down. MA seeks by starting the stream
     * again at the place, so any player can; it turns one down (12, "invalid command") while the
     * song isn't under way yet, which is no reason to stop trying.
     */
    private suspend fun seekTo(positionMs: Long): Boolean {
        if (cantSeek) return false
        return try {
            queue("player_queues/seek", JSONObject().put("position", positionMs / 1000))
            true
        } catch (e: MaException) {
            when (e.code) {
                9 -> { cantSeek = true; Log.w(TAG, "$name can't seek (${e.message})") }
                12 -> Log.i(TAG, "$name turned a seek down for now (${e.message})")
                else -> throw e
            }
            false
        }
    }

    override fun close() {
        listening?.cancel()
        scope.launch {
            runCatching { queue("player_queues/stop") }
            client.release()
            scope.cancel()
        }
    }

    private suspend fun queue(command: String, args: JSONObject = JSONObject()) = client.call(command, args.put("queue_id", queueId))

    private suspend fun command(block: suspend () -> Unit) = withContext(Dispatchers.IO) {
        runCatching { block() }.onFailure { Log.w(TAG, "command to $name failed: ${it.message}") }
        Unit
    }

    // ---- What it's doing ---------------------------------------------------------

    internal fun onEvent(e: MaEvent) {
        when (e.event) {
            MaClient.CONNECTED -> scope.launch { refresh() }
            "player_updated" -> if (e.objectId == playerId) (e.data as? JSONObject)?.let(::onPlayer)
            "player_removed" -> if (e.objectId == playerId) {
                _status.value = _status.value.copy(state = RemoteState.GONE, at = SystemClock.elapsedRealtime())
            }
            "queue_updated" -> if (e.objectId == queueId) (e.data as? JSONObject)?.let(::onQueue)
            "queue_items_updated" -> if (e.objectId == queueId) {
                (e.data as? JSONObject)?.let(::onQueue)
                reloadItems()
            }
            "queue_time_updated" -> if (e.objectId == queueId) (e.data as? Number)?.let { seconds ->
                val q = _queue.value
                if (q.state == "idle") return
                _queue.value = q.copy(positionMs = (seconds.toDouble() * 1000).toLong(), at = SystemClock.elapsedRealtime())
                updateStatus()
            }
        }
    }

    private fun onPlayer(p: JSONObject) {
        val volume = if (p.isNull("volume_level")) _status.value.volume else p.optInt("volume_level") / 100f
        if (p.has("available") && !p.optBoolean("available", true)) {
            _status.value = _status.value.copy(state = RemoteState.GONE, volume = volume, at = SystemClock.elapsedRealtime())
            return
        }
        _status.value = _status.value.copy(volume = volume)
    }

    internal fun onQueue(q: JSONObject) {
        val old = _queue.value
        val now = SystemClock.elapsedRealtime()
        val state = q.optString("state").ifBlank { "idle" }
        val itemId = q.optJSONObject("current_item")?.optString("queue_item_id")?.ifBlank { null }
        val elapsed = (q.optDouble("elapsed_time", 0.0) * 1000).toLong()
        // elapsed_time is as of elapsed_time_last_updated, in the server's unix seconds.
        val since = q.optDouble("elapsed_time_last_updated", 0.0).takeIf { it > 0 }
            ?.let { (System.currentTimeMillis() - it * 1000).toLong().coerceIn(0, 10_000) } ?: 0
        // A stopped queue (an Echo Show stops rather than pauses) reports 0:00; the song is held where it was.
        val stoppedHere = state == "idle" && elapsed == 0L && itemId != null && itemId == old.itemId
        _queue.value = MaQueue(
            state = state,
            index = if (q.isNull("current_index")) -1 else q.optInt("current_index", -1),
            itemId = itemId,
            positionMs = if (stoppedHere) old.positionNow(now) else if (state == "playing") elapsed + since else elapsed,
            at = now,
            shuffle = q.optBoolean("shuffle_enabled"),
            repeat = q.optString("repeat_mode").ifBlank { "off" },
            count = q.optInt("items", _items.value.size),
        )
        // Songs it has that aren't read yet (or no longer has): read again.
        if (q.has("items") && q.optInt("items") != _items.value.size || itemId != null && _items.value.none { it.id == itemId }) reloadItems()
        updateStatus()
    }

    /** The queue as the cast sheet sees it. */
    private fun updateStatus() {
        val q = _queue.value
        val item = _items.value.firstOrNull { it.id == q.itemId }
        _status.value = RemoteStatus(
            state = when {
                _status.value.state == RemoteState.GONE -> RemoteState.GONE
                q.state == "playing" -> RemoteState.PLAYING
                q.state == "paused" -> RemoteState.PAUSED
                else -> RemoteState.IDLE
            },
            positionMs = q.positionMs,
            at = q.at,
            volume = _status.value.volume,
            mediaId = item?.youTubeId ?: item?.uri,
        )
    }

    companion object {
        private const val TAG = "Cast"
        /** How far into a song the phone must be for it to start there on the speaker: a seek starts MA's stream over. */
        private const val SEEK_MIN_MS = 5_000L
        private const val START_SEEK_TRIES = 8
        private const val START_SEEK_GAP_MS = 1_000L
        /** How many songs go in each `play_media` after the first. */
        private const val BATCH = 25
        /** How many of the queue's songs are read. */
        private const val ITEMS_MAX = 500
        private const val ITEMS_SETTLE_MS = 300L

        /** How a YouTube song is handed to MA's YouTube Music provider, which fetches it itself. */
        fun youTubeUri(videoId: String, provider: String = "ytmusic") = "$provider://track/$videoId"

        /**
         * The provider, of [providers] as MA lists them, that a YouTube song goes to: one working
         * (available), whose domain is YouTube Music's own or one built on it (`ytmusic_free`),
         * the official one first. MA reads the address's scheme as a provider instance or a
         * domain, so the instance is named, which is the one that's working.
         */
        internal fun youTubeMusicOf(providers: List<JSONObject>): String? = providers
            .filter { it.optString("domain").startsWith("ytmusic") && it.optBoolean("available", true) }
            .sortedBy { if (it.optString("domain") == "ytmusic") 0 else 1 }
            .firstNotNullOfOrNull { p -> p.optString("instance_id").ifBlank { null } ?: p.optString("domain").ifBlank { null } }

        /** The YouTube id in a YouTube Music provider's address (`ytmusic_free--a1b2://track/<id>`), or null. */
        internal fun youTubeIdOf(uri: String?): String? =
            uri?.let { Regex("^ytmusic[a-z_]*(--[^:/]+)?://track/([A-Za-z0-9_-]{11})$").find(it) }?.groupValues?.get(2)

        /** A queue item as MA lists it. */
        internal fun itemOf(item: JSONObject): MaItem? {
            val id = item.optString("queue_item_id").ifBlank { null } ?: return null
            val track = item.optJSONObject("media_item")
            val artists = track?.optJSONArray("artists")
            val image = item.optJSONObject("image")?.optString("path")
                ?: track?.optJSONObject("metadata")?.optJSONArray("images")?.optJSONObject(0)?.optString("path")
            val seconds = item.optDouble("duration", 0.0).takeIf { it > 0 } ?: track?.optDouble("duration", 0.0) ?: 0.0
            return MaItem(
                id = id,
                uri = uriOf(item),
                title = track?.optString("name")?.ifBlank { null } ?: item.optString("name"),
                artist = artists?.let { a -> (0 until a.length()).mapNotNull { a.optJSONObject(it)?.optString("name")?.ifBlank { null } } }.orEmpty().joinToString(", "),
                album = track?.optJSONObject("album")?.optString("name")?.ifBlank { null },
                imageUrl = image?.takeIf { it.startsWith("http") },
                durationMs = (seconds * 1000).toLong(),
            )
        }

        /**
         * A song played from an address, as a whole track for MA's built-in provider. Given
         * only the address, MA names a track from the stream's own tags, and the phone's
         * streams have none, so the speaker showed the address. With an image, MA keeps
         * the title, artist and artwork sent here.
         */
        internal fun builtinTrack(media: CastMedia): JSONObject {
            fun mapping(type: String, name: String) = JSONObject()
                .put("item_id", name).put("provider", "builtin").put("name", name).put("media_type", type)
            val track = JSONObject()
                .put("item_id", media.url)
                .put("provider", "builtin")
                .put("name", media.title)
                .put("media_type", "track")
                .put("provider_mappings", JSONArray().put(JSONObject()
                    .put("item_id", media.url).put("provider_domain", "builtin").put("provider_instance", "builtin").put("available", true)))
            if (media.durationMs > 0) track.put("duration", media.durationMs / 1000)
            if (media.artist.isNotBlank()) track.put("artists", JSONArray().put(mapping("artist", media.artist)))
            media.album?.takeIf { it.isNotBlank() }?.let { track.put("album", mapping("album", it)) }
            media.imageUrl?.let { image ->
                track.put("metadata", JSONObject().put("images", JSONArray().put(JSONObject()
                    .put("type", "thumb").put("path", image).put("provider", "builtin").put("remotely_accessible", true))))
            }
            return track
        }

        /** The URI of a queue item, from its media item (or MA's own link for it). */
        internal fun uriOf(item: JSONObject): String? =
            item.optJSONObject("media_item")?.optString("uri")?.ifBlank { null }
                ?: item.optJSONObject("streamdetails")?.optString("uri")?.ifBlank { null }

        /**
         * Whether a queue item's [uri] is the song sent as [sent]. MA may name a provider
         * instance in the scheme (`ytmusic--a1b2://track/…`) and wraps a plain address as
         * one of its own, with the address inside, so the two are compared loosely.
         */
        internal fun sameMedia(uri: String, sent: String): Boolean {
            if (uri == sent) return true
            fun bare(u: String) = u.replaceFirst(Regex("^([a-z_]+)--[^:]+://"), "$1://")
            if (bare(uri) == bare(sent)) return true
            if (sent.startsWith("http")) {
                val decoded = runCatching { URLDecoder.decode(uri, "UTF-8") }.getOrDefault(uri)
                return sent in decoded || sent.substringAfterLast('/').let { it.length > 8 && it in decoded }
            }
            val track = sent.substringAfter("://track/", "").takeIf { it.isNotEmpty() } ?: return false
            return uri.endsWith("/track/$track")
        }
    }
}
