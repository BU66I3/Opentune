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
    /** Whether the server has a YouTube Music provider set up, so songs can be handed over by id. */
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

/**
 * A Music Assistant player as a cast target. OpenTune hands its queue's current
 * song to the player's MA queue, one song at a time ("replace"), and the phone's
 * own player stays the queue, as with every other receiver (see
 * [CastMirror][com.opentune.playback.CastMirror]). So that MA doesn't carry on
 * with songs of its own after each one, its "don't stop the music" is turned off
 * while OpenTune is attached, and put back as it was after.
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
    @Volatile private var loaded: CastMedia? = null
    /** The queue item on the player when the last song was sent, and the one MA made of that song. */
    @Volatile private var itemBefore: String? = null
    @Volatile private var ourItem: String? = null
    @Volatile private var currentItem: String? = null
    /** The loaded song was seen playing, so the queue going idle after is its end. */
    @Volatile private var played = false
    /** When the song was sent, for the log of how long the speaker took to start it. */
    @Volatile private var sentAt = 0L
    @Volatile private var autoplayWas: Boolean? = null
    private var listening: Job? = null

    val localAddress: InetAddress? get() = (client as? MaClient)?.localAddress

    /** Set once MA's YouTube Music has failed to play a song here; songs come through the phone from then on. */
    @Volatile var youTubeMusicFailed = false
        private set

    /** Whether the server can fetch YouTube songs itself. */
    /** The server's YouTube Music provider, to send a YouTube song to by its id; null to send it through the phone. */
    suspend fun youTubeMusic(): String? = if (youTubeMusicFailed) null else client.youTubeMusicProvider()

    /** Starts following the player and turns MA's own autoplay off for its queue. */
    suspend fun attach() = withContext(Dispatchers.IO) {
        // Subscribed before going on, so nothing MA says from here on is missed.
        listening = scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { client.events.collect(::onEvent) }
        refresh()
        runCatching { client.call("player_queues/dont_stop_the_music", JSONObject().put("queue_id", queueId).put("dont_stop_the_music_enabled", false)) }
            .onFailure { Log.w(TAG, "couldn't turn off autoplay on $name: ${it.message}") }
        Unit
    }

    /** Reads the queue and player as they are now. */
    private suspend fun refresh() {
        val queue = runCatching { client.call("player_queues/all") as? JSONArray }.getOrNull()
        queue?.let { all -> (0 until all.length()).map { all.getJSONObject(it) }.firstOrNull { it.optString("queue_id") == queueId } }?.let {
            if (autoplayWas == null) autoplayWas = autoplayOf(it)
            onQueue(it)
        }
        val players = runCatching { client.call("players/all") as? JSONArray }.getOrNull()
        players?.let { all -> (0 until all.length()).map { all.getJSONObject(it) }.firstOrNull { it.optString("player_id") == playerId } }?.let(::onPlayer)
    }

    override suspend fun load(media: CastMedia, positionMs: Long, play: Boolean) = withContext(Dispatchers.IO) {
        wantPlay = play
        // The speaker already moved on to this song (its Next, or the end of the last one):
        // it's playing, so it's taken as loaded rather than sent again.
        val adopted = movedTo
        movedTo = null
        queuedNext = null
        if (adopted != null && adopted.second.id == media.id) {
            loaded = media
            sentAt = 0L
            ourItem = adopted.first
            itemBefore = null
            played = false
            _status.value = RemoteStatus(RemoteState.BUFFERING, 0, SystemClock.elapsedRealtime(), _status.value.volume, media.id)
            if (positionMs >= SEEK_MIN_MS || !play) scope.launch {
                if (positionMs >= SEEK_MIN_MS) runCatching { seekOnceUnderWay(positionMs, adopted.first) }
                if (!wantPlay) runCatching { queue("player_queues/pause") }
            }
            return@withContext
        }
        loaded = media
        played = false
        sentAt = SystemClock.elapsedRealtime()
        itemBefore = currentItem
        ourItem = null
        // play_media replaces the whole queue, so nothing sent before is in it any more.
        sent.clear()
        _status.value = RemoteStatus(RemoteState.LOADING, positionMs, SystemClock.elapsedRealtime(), _status.value.volume, media.id)
        try {
            // Where to start, and whether paused, are sent once MA's queue holds the song:
            // play_media returns before it does, and a seek to an empty queue is refused.
            startAt = positionMs.takeIf { it >= SEEK_MIN_MS }
            playMedia(media)
        } catch (e: Exception) {
            Log.w(TAG, "$name wouldn't play ${media.title}: ${e.message}")
            _status.value = _status.value.copy(state = if (e is MaException && e.signIn) RemoteState.GONE else RemoteState.ERROR, at = SystemClock.elapsedRealtime())
        }
    }

    override val takesNext: Boolean get() = true

    /** The phone's next song, queued after the current one in MA. */
    @Volatile private var queuedNext: CastMedia? = null
    /**
     * The queue item the speaker went to by itself (its Next or Previous, or the end of a song),
     * and the song it is; loading that song then takes it as already playing.
     */
    @Volatile private var movedTo: Pair<String, CastMedia>? = null
    /** The songs sent here, by the queue item MA made of each, so the speaker going back to one is known. */
    private val sent = java.util.concurrent.ConcurrentHashMap<String, CastMedia>()

    /** Puts the phone's next song after the current one, once the current one is in MA's queue. */
    override suspend fun preload(media: CastMedia) = withContext(Dispatchers.IO) {
        queuedNext = media
        if (ourItem != null) sendNext(media)
    }

    /** `replace_next`: MA's queue then holds the current song and this one after it, and nothing else. */
    private suspend fun sendNext(media: CastMedia) {
        fun args(item: Any) = JSONObject().put("queue_id", queueId).put("media", JSONArray().put(item)).put("option", "replace_next")
        fun address(m: CastMedia): Any = if (m.url.startsWith("http") && !m.live && !addressesOnly) builtinTrack(m) else m.url
        val backup = media.fallbackUrl
        val viaPhone = backup?.let { media.copy(url = it, fallbackUrl = null) }
        try {
            if (viaPhone != null && !media.url.startsWith("http") && !youTubeMusicFailed) {
                try {
                    client.call("player_queues/play_media", args(media.url))
                    return
                } catch (e: MaException) {
                    if (e.signIn) throw e
                    youTubeMusicFailed = true
                }
            }
            client.call("player_queues/play_media", args(address(viaPhone ?: media)))
        } catch (e: Exception) {
            // Without it, the speaker's own Next does nothing; the phone's Next still works.
            Log.w(TAG, "couldn't queue the next song on $name: ${e.message}")
        }
    }

    /** Where the song just sent should start, and whether paused, once MA's queue holds it. */
    @Volatile private var startAt: Long? = null
    /**
     * Whether the phone last asked for it playing or paused. A seek waiting for MA, as a song
     * starts, ends with a pause only if paused is still what's wanted: a pause decided when the
     * song was sent paused the speaker after the phone had been played meanwhile.
     */
    @Volatile private var wantPlay = true

    /** The queue last said it was idle: stopped, with nothing to resume. */
    @Volatile private var queueIdle = false

    override suspend fun play() = command {
        wantPlay = true
        val media = loaded
        // A stopped queue (an Echo Show stops rather than pauses) is resumed by MA from where it
        // stopped, the server starting the stream there, so it works on a speaker that can't seek.
        // Sending the song again started it from the top on such a speaker.
        val resumed = try {
            queue("player_queues/play")
            true
        } catch (e: MaException) {
            if (e.signIn || !queueIdle || media == null) throw e
            Log.w(TAG, "$name wouldn't resume (${e.message}); sending the song again")
            false
        }
        if (!resumed && media != null) {
            // Nothing left to resume: the song is sent again and moved to its place.
            val at = _status.value.positionNow()
            ourItem = null
            itemBefore = currentItem
            startAt = at.takeIf { it >= SEEK_MIN_MS }
            playMedia(media)
        }
    }
    override suspend fun pause() = command {
        wantPlay = false
        queue("player_queues/pause")
    }

    /** Set once the player has turned a seek down as unsupported; none are sent after. */
    @Volatile private var cantSeek = false

    /**
     * Moves the song to [positionMs]; false if MA turned it down. MA seeks by starting the
     * stream again at the place, so any player can; it turns one down (12, its catch-all
     * "invalid command") while the song isn't under way yet. That was taken as a player that
     * can't seek, and a seek in a song's first moments turned seeking off for good.
     */
    private suspend fun seekTo(positionMs: Long): Boolean {
        if (cantSeek) return false
        return try {
            queue("player_queues/seek", JSONObject().put("position", positionMs / 1000))
            true
        } catch (e: MaException) {
            when (e.code) {
                // An unsupported feature: so it stays.
                9 -> { cantSeek = true; Log.w(TAG, "$name can't seek (${e.message})") }
                12 -> Log.i(TAG, "$name turned a seek down for now (${e.message})")
                else -> throw e
            }
            false
        }
    }

    /**
     * A seek as a song starts, tried again while MA isn't ready for it; while it's meant to be
     * playing, the place moves on with the time spent waiting, as the phone's does.
     */
    private suspend fun seekOnceUnderWay(positionMs: Long, item: String?) {
        val from = SystemClock.elapsedRealtime()
        repeat(START_SEEK_TRIES) { attempt ->
            if (attempt > 0) kotlinx.coroutines.delay(START_SEEK_GAP_MS)
            if (cantSeek || ourItem != item) return
            val waited = if (wantPlay) SystemClock.elapsedRealtime() - from else 0
            if (seekTo(positionMs + waited)) return
        }
    }
    override suspend fun seek(positionMs: Long) = command {
        if (!seekTo(positionMs)) return@command
        _status.value = _status.value.copy(positionMs = positionMs, at = SystemClock.elapsedRealtime())
    }

    override suspend fun setVolume(level: Float) = command {
        val percent = (level.coerceIn(0f, 1f) * 100).roundToInt()
        client.call("players/cmd/volume_set", JSONObject().put("player_id", playerId).put("volume_level", percent))
        _status.value = _status.value.copy(volume = percent / 100f)
    }

    override fun close() {
        listening?.cancel()
        scope.launch {
            runCatching { queue("player_queues/stop") }
            autoplayWas?.let { was ->
                runCatching { client.call("player_queues/dont_stop_the_music", JSONObject().put("queue_id", queueId).put("dont_stop_the_music_enabled", was)) }
            }
            client.release()
            scope.cancel()
        }
    }

    private suspend fun queue(command: String, args: JSONObject = JSONObject()) = client.call(command, args.put("queue_id", queueId))

    /** Set once the server has turned down a song sent as a whole track; addresses alone go from then on. */
    @Volatile private var addressesOnly = false

    /** Replaces what the player's queue holds with [media]. */
    private suspend fun playMedia(media: CastMedia) {
        fun args(item: Any) = JSONObject().put("queue_id", queueId).put("media", JSONArray().put(item)).put("option", "replace")
        val backup = media.fallbackUrl
        // Failed here once already (a song sent again after a stop, say): straight through the phone.
        if (backup != null && !media.url.startsWith("http") && youTubeMusicFailed) {
            return playMedia(media.copy(url = backup, fallbackUrl = null))
        }
        if (backup != null && !media.url.startsWith("http")) {
            try {
                client.call("player_queues/play_media", args(media.url))
                return
            } catch (e: MaException) {
                if (e.signIn) throw e
                // MA's YouTube Music couldn't play it (signed out, or turned away by YouTube): through the phone, then.
                Log.w(TAG, "$name couldn't play ${media.title} from YouTube Music (${e.message}); playing it through the phone")
                if (!youTubeMusicFailed) {
                    youTubeMusicFailed = true
                    Cast.say(com.opentune.i18n.Tr.t("Music Assistant's YouTube Music couldn't play this, so songs play through this phone"))
                }
                return playMedia(media.copy(url = backup, fallbackUrl = null))
            }
        }
        if (media.url.startsWith("http") && !media.live && !addressesOnly) {
            try {
                client.call("player_queues/play_media", args(builtinTrack(media)))
                return
            } catch (e: MaException) {
                if (e.signIn) throw e
                Log.w(TAG, "$name wouldn't take ${media.title} with its details (${e.message}); sending the address alone")
                addressesOnly = true
            }
        }
        client.call("player_queues/play_media", args(media.url))
    }

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
            "queue_time_updated" -> if (e.objectId == queueId) (e.data as? Number)?.let { seconds ->
                if (_status.value.state == RemoteState.LOADING) return
                _status.value = _status.value.copy(positionMs = (seconds.toDouble() * 1000).toLong(), at = SystemClock.elapsedRealtime())
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

    private fun onQueue(q: JSONObject) {
        val media = loaded
        val current = q.optJSONObject("current_item")
        val uri = current?.let(::uriOf)
        val itemId = current?.optString("queue_item_id")?.ifBlank { null }
        currentItem = itemId
        // The speaker went from our song to another by itself: back to one sent before (its
        // Previous), or on to the next one queued (its Next, or the end of the song). It's
        // reported as playing that song, so the phone goes to it too, either way.
        fun isSong(m: CastMedia) = uri == null || sameMedia(uri, m.url) || m.fallbackUrl?.let { sameMedia(uri, it) } == true
        // Our song again as a new item (MA starting it over) is still ours, not the speaker moving on.
        if (media != null && ourItem != null && itemId != null && itemId != ourItem && sent[itemId] == null && uri != null && isSong(media)) {
            ourItem = itemId
            sent[itemId] = media
        }
        // An item never seen before is the next song queued only if it is that song.
        val moved = if (media != null && ourItem != null && itemId != null && itemId != ourItem) sent[itemId] ?: queuedNext?.takeIf(::isSong) else null
        if (moved != null && movedTo?.first != itemId) {
            Log.i(TAG, "$name went to another song by itself")
            movedTo = itemId!! to moved
            sent[itemId] = moved
        }
        // The song sent is the first new item to turn up, or one with its address.
        if (media != null && ourItem == null && itemId != null && itemId != itemBefore) {
            ourItem = itemId
            sent[itemId] = media
            // The song is in MA's queue now: it can be moved to its place, and paused if it should be,
            // and the phone's next song put after it.
            val startFrom = startAt
            val next = queuedNext
            startAt = null
            if (startFrom != null || !wantPlay) scope.launch {
                startFrom?.let { runCatching { seekOnceUnderWay(it, itemId) }.onFailure { e -> Log.w(TAG, "seek on $name failed: ${e.message}") } }
                // A seek starts it playing again; paused only if that's still what's wanted.
                if (!wantPlay) runCatching { queue("player_queues/pause") }
            }
            // Not held up by a seek waiting for MA.
            next?.let { scope.launch { sendNext(it) } }
        }
        val ours = media != null && itemId != null && itemId == ourItem
        val mediaId = when {
            ours -> media!!.id
            moved != null -> moved.id
            uri != null -> uri
            else -> media?.id
        }
        val state = when (q.optString("state")) {
            "playing" -> if (ours || moved != null || media == null) RemoteState.PLAYING.also {
                if (ours && !played && sentAt > 0) Log.i(TAG, "$name started ${media?.title} ${SystemClock.elapsedRealtime() - sentAt} ms after it was sent")
                played = played || ours
            } else RemoteState.LOADING
            "paused" -> RemoteState.PAUSED
            // Idle is the end of the song only when MA says so (`ended`), or, from a server
            // without that flag, when it stopped in the song's last seconds. Otherwise it was
            // stopped: many players stop rather than pause, and that was taken as the end,
            // so pausing skipped the song. It's held as paused, where it stopped.
            "idle" -> when {
                media != null && played && (ours || current == null) && (q.optBoolean("ended") || !q.has("ended") && nearEnd(media)) -> RemoteState.FINISHED
                media != null && played && ours -> RemoteState.PAUSED
                _status.value.state == RemoteState.LOADING -> RemoteState.LOADING
                else -> RemoteState.IDLE
            }
            else -> _status.value.state
        }
        queueIdle = q.optString("state") == "idle"
        val finished = media != null && ours && q.optBoolean("ended")
        val at = SystemClock.elapsedRealtime()
        // elapsed_time is as of elapsed_time_last_updated, in the server's unix seconds.
        val elapsed = (q.optDouble("elapsed_time", 0.0) * 1000).toLong()
        val since = q.optDouble("elapsed_time_last_updated", 0.0).takeIf { it > 0 }?.let { (System.currentTimeMillis() - it * 1000).toLong().coerceIn(0, 10_000) } ?: 0
        val position = if (state == RemoteState.PLAYING) elapsed + since else elapsed
        _status.value = RemoteStatus(
            state = if (finished) RemoteState.FINISHED else state,
            // A stopped queue reports 0:00; the song is held where it was.
            positionMs = if (state == RemoteState.LOADING || queueIdle && state == RemoteState.PAUSED) _status.value.positionNow(at) else position,
            at = at,
            volume = _status.value.volume,
            mediaId = mediaId,
        )
    }

    /** Whether the loaded song was last seen in its final seconds. */
    private fun nearEnd(media: CastMedia): Boolean =
        media.durationMs > 0 && _status.value.positionNow() >= media.durationMs - END_MS

    companion object {
        private const val TAG = "Cast"
        private const val SEEK_MIN_MS = 1_000L
        /** How often, and how far apart, a seek as a song starts is tried while MA isn't ready for it. */
        private const val START_SEEK_TRIES = 5
        private const val START_SEEK_GAP_MS = 1_000L
        /** How near its end a song that stops counts as finished, on a server that doesn't say. */
        private const val END_MS = 5_000L

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

        internal fun autoplayOf(q: JSONObject): Boolean? = when {
            q.has("dont_stop_the_music_enabled") -> q.optBoolean("dont_stop_the_music_enabled")
            q.has("autoplay_enabled") -> q.optBoolean("autoplay_enabled")
            else -> null
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
