package com.opentune.cast

import java.util.Collections
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.runBlocking
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = android.app.Application::class)
class MusicAssistantTest {
    /** A pretend server: records each command and answers the reads from fixed JSON. */
    private class FakeMa(private val queues: String = QUEUES, private val players: String = PLAYERS) : MaApi {
        val calls: MutableList<String> = Collections.synchronizedList(mutableListOf<String>())
        val flow = MutableSharedFlow<MaEvent>(extraBufferCapacity = 64)
        var released = 0
        var fail: MaException? = null
        /** Turns down the commands it returns an error for. */
        var refuse: ((String) -> MaException?)? = null
        override val events: SharedFlow<MaEvent> = flow
        override suspend fun call(command: String, args: JSONObject): Any? {
            val line = "$command ${args.toString().replace("\\/", "/")}"
            calls += line
            fail?.let { throw it }
            refuse?.invoke(line)?.let { throw it }
            return when (command) {
                "player_queues/all" -> JSONArray(queues)
                "players/all" -> JSONArray(players)
                else -> null
            }
        }
        override fun release() { released++ }

        /** An event as the server sends it, read the way [MaClient] reads one. */
        fun replay(raw: String) {
            val json = JSONObject(raw)
            flow.tryEmit(MaEvent(json.getString("event"), json.optString("object_id").ifBlank { null }, json.opt("data")))
        }
    }

    private fun media(id: String = "dQw4w9WgXcQ") = CastMedia(id, MusicAssistant.youTubeUri(id), "audio/mp4", "T", "A", null, null, 200_000)

    /** Waits for a status, since events are taken in on the receiver's own threads. */
    private fun MusicAssistant.waitFor(state: RemoteState): RemoteStatus {
        val deadline = System.currentTimeMillis() + 3_000
        while (System.currentTimeMillis() < deadline && status.value.state != state) Thread.sleep(20)
        return status.value
    }

    @Test fun loadSendsTheSongAndTurnsAutoplayOff() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking {
            kitchen.attach()
            kitchen.load(media(), 42_500, play = false)
            kitchen.setVolume(0.62f)
        }
        fun sent() = ma.calls.filterNot { it.startsWith("player_queues/all") || it.startsWith("players/all") }
        // Nothing to seek or pause yet: MA's play_media returns before its queue holds the song,
        // and a seek to an empty queue is refused ("There is nothing to play here").
        assertEquals(
            listOf(
                """player_queues/dont_stop_the_music {"queue_id":"media_player.kitchen","dont_stop_the_music_enabled":false}""",
                """player_queues/play_media {"queue_id":"media_player.kitchen","media":["ytmusic://track/dQw4w9WgXcQ"],"option":"replace"}""",
                """players/cmd/volume_set {"player_id":"media_player.kitchen","volume_level":62}""",
            ),
            sent(),
        )
        // The song turns up in the queue: now it's moved to its place and paused.
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic://track/dQw4w9WgXcQ", 0.0))
        val deadline = System.currentTimeMillis() + 3_000
        while (System.currentTimeMillis() < deadline && sent().size < 5) Thread.sleep(20)
        assertEquals(
            listOf(
                """player_queues/seek {"position":42,"queue_id":"media_player.kitchen"}""",
                """player_queues/pause {"queue_id":"media_player.kitchen"}""",
            ),
            sent().drop(3),
        )
        assertEquals(0.62f, kitchen.status.value.volume!!, 0.001f)
        assertEquals("dQw4w9WgXcQ", kitchen.status.value.mediaId)
    }

    @Test fun followsTheQueueToTheEndOfTheSong() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking {
            kitchen.attach()
            kitchen.load(media(), 0, play = true)
        }
        assertEquals(RemoteState.LOADING, kitchen.status.value.state)
        // The old song is still reported for a moment: not taken as the new one.
        ma.replay(queueEvent("media_player.kitchen", "playing", "old-item", "ytmusic://track/oldSong", 80.0))
        Thread.sleep(100)
        assertEquals(RemoteState.LOADING, kitchen.status.value.state)
        // Then MA's item for the song sent, with its provider instance in the scheme.
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic--x7Qk://track/dQw4w9WgXcQ", 3.0))
        val playing = kitchen.waitFor(RemoteState.PLAYING)
        assertEquals("dQw4w9WgXcQ", playing.mediaId)
        assertTrue(playing.positionMs in 3_000L..4_000L)
        // Another queue's events are none of its business.
        ma.replay(queueEvent("media_player.lounge", "paused", "x", "ytmusic://track/other", 1.0))
        ma.replay("""{"event":"queue_time_updated","object_id":"media_player.lounge","data":99}""")
        ma.replay("""{"event":"queue_time_updated","object_id":"media_player.kitchen","data":61.5}""")
        Thread.sleep(100)
        assertEquals(RemoteState.PLAYING, kitchen.status.value.state)
        assertEquals(61_500L, kitchen.status.value.positionMs)
        // Paused from Home Assistant.
        ma.replay(queueEvent("media_player.kitchen", "paused", "new-item", "ytmusic://track/dQw4w9WgXcQ", 61.5))
        assertEquals(RemoteState.PAUSED, kitchen.waitFor(RemoteState.PAUSED).state)
        ma.replay("""{"event":"player_updated","object_id":"media_player.kitchen","data":{"player_id":"media_player.kitchen","available":true,"playback_state":"paused","volume_level":35}}""")
        Thread.sleep(100)
        assertEquals(0.35f, kitchen.status.value.volume!!, 0.001f)
        // Played to the end: the queue stops on the same item, marked ended.
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic://track/dQw4w9WgXcQ", 190.0))
        ma.replay(queueEvent("media_player.kitchen", "idle", "new-item", "ytmusic://track/dQw4w9WgXcQ", 0.0, ended = true))
        val done = kitchen.waitFor(RemoteState.FINISHED)
        assertEquals(RemoteState.FINISHED, done.state)
        assertEquals("dQw4w9WgXcQ", done.mediaId)
    }

    @Test fun aSongFromThePhoneGoesWithItsDetails() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        val url = "http://192.168.1.20:41234/s/a2c1e1862f4e.m4a"
        val song = CastMedia("song", url, "audio/mp4", "Night Swim", "Bonobo", "Fragments", "https://lh3.example/cover.jpg", 194_000)
        runBlocking {
            kitchen.attach()
            kitchen.load(song, 0, play = true)
        }
        val sent = JSONObject(ma.calls.single { it.startsWith("player_queues/play_media") }.substringAfter(' '))
        val track = sent.getJSONArray("media").getJSONObject(0)
        assertEquals("Night Swim", track.getString("name"))
        assertEquals("track", track.getString("media_type"))
        assertEquals(url, track.getString("item_id"))
        assertEquals("builtin", track.getJSONArray("provider_mappings").getJSONObject(0).getString("provider_domain"))
        assertEquals("Bonobo", track.getJSONArray("artists").getJSONObject(0).getString("name"))
        assertEquals("Fragments", track.getJSONObject("album").getString("name"))
        assertEquals(194, track.getInt("duration"))
        assertEquals("https://lh3.example/cover.jpg", track.getJSONObject("metadata").getJSONArray("images").getJSONObject(0).getString("path"))
        // MA's own item for it, `builtin://track/<address>`, is taken as the song sent.
        ma.replay(queueEvent("media_player.kitchen", "playing", "url-item", "builtin://track/$url", 1.0))
        assertEquals("song", kitchen.waitFor(RemoteState.PLAYING).mediaId)
    }

    @Test fun aServerThatWontTakeDetailsGetsTheAddress() {
        val ma = FakeMa()
        ma.refuse = { line -> if (line.startsWith("player_queues/play_media") && "\"name\"" in line) MaException(3, "Invalid data") else null }
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        val url = "http://192.168.1.20:41234/s/a2c1e1862f4e.m4a"
        val song = CastMedia("song", url, "audio/mp4", "Night Swim", "Bonobo", null, "https://lh3.example/cover.jpg", 194_000)
        runBlocking {
            kitchen.attach()
            kitchen.load(song, 0, play = true)
            kitchen.load(song.copy(id = "next"), 0, play = true)
        }
        val plays = ma.calls.filter { it.startsWith("player_queues/play_media") }
        // Turned down once with its details, then the address alone, and from then on the address alone.
        assertEquals(3, plays.size)
        assertTrue("\"name\"" in plays[0])
        assertTrue(plays[1].endsWith(""""media":["$url"],"option":"replace"}"""))
        assertTrue(plays[2].endsWith(""""media":["$url"],"option":"replace"}"""))
        assertEquals(RemoteState.LOADING, kitchen.status.value.state)
    }

    /** A player that stops rather than pauses: MA's queue goes idle, without `ended`. That isn't the end of the song. */
    @Test fun pausingIsNotTheEndOfTheSong() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking {
            kitchen.attach()
            kitchen.load(media(), 0, play = true)
        }
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic://track/dQw4w9WgXcQ", 61.0))
        kitchen.waitFor(RemoteState.PLAYING)
        ma.replay(queueEvent("media_player.kitchen", "idle", "new-item", "ytmusic://track/dQw4w9WgXcQ", 0.0, ended = false))
        val paused = kitchen.waitFor(RemoteState.PAUSED)
        assertEquals(RemoteState.PAUSED, paused.state)
        assertEquals("dQw4w9WgXcQ", paused.mediaId)
        // Held where it stopped, not at the 0:00 the stopped queue reports.
        assertTrue(paused.positionMs in 61_000L..63_000L)
        // Play sends the song again and moves it back to its place.
        ma.calls.clear()
        runBlocking { kitchen.play() }
        assertTrue(ma.calls[0].startsWith("player_queues/play_media"))
        ma.replay(queueEvent("media_player.kitchen", "playing", "again-item", "ytmusic://track/dQw4w9WgXcQ", 0.0))
        val deadline = System.currentTimeMillis() + 3_000
        while (System.currentTimeMillis() < deadline && ma.calls.none { it.startsWith("player_queues/seek") }) Thread.sleep(20)
        assertTrue(ma.calls.first { it.startsWith("player_queues/seek") }.startsWith("player_queues/seek {\"position\":6"))
    }

    /** MA lists a YouTube Music provider that can't play the song: it comes through the phone instead. */
    @Test fun aFailingYouTubeMusicFallsBackToThePhone() {
        val ma = FakeMa()
        ma.refuse = { line -> if ("ytmusic://" in line) MaException(8, "There is nothing to play here.") else null }
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        val backup = "http://192.168.1.20:41234/s/backup.m4a"
        runBlocking {
            kitchen.attach()
            kitchen.load(media().copy(title = "Night Swim", artist = "Bonobo", imageUrl = "https://lh3.example/c.jpg", fallbackUrl = backup), 0, play = true)
        }
        val plays = ma.calls.filter { it.startsWith("player_queues/play_media") }
        assertEquals(2, plays.size)
        assertTrue("ytmusic://track/dQw4w9WgXcQ" in plays[0])
        val track = JSONObject(plays[1].substringAfter(' ')).getJSONArray("media").getJSONObject(0)
        assertEquals(backup, track.getString("item_id"))
        assertEquals("Night Swim", track.getString("name"))
        assertEquals(RemoteState.LOADING, kitchen.status.value.state)
        // From then on, songs for this speaker come through the phone.
        assertFalse(runBlocking { kitchen.youTubeMusic() })
    }

    /** A player that can't seek (an Echo Show) says so once; no more seeks are sent to it. */
    @Test fun aPlayerThatCantSeekIsAskedOnce() {
        val ma = FakeMa()
        ma.refuse = { line -> if (line.startsWith("player_queues/seek")) MaException(12, "Invalid or unsupported command.") else null }
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking {
            kitchen.attach()
            kitchen.load(media(), 0, play = true)
            kitchen.seek(30_000)
            kitchen.seek(60_000)
        }
        assertEquals(1, ma.calls.count { it.startsWith("player_queues/seek") })
    }

    @Test fun aPlayerTakenAwayIsGone() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking { kitchen.attach() }
        ma.replay("""{"event":"player_removed","object_id":"media_player.kitchen","data":null}""")
        assertEquals(RemoteState.GONE, kitchen.waitFor(RemoteState.GONE).state)
    }

    @Test fun aRefusedTokenEndsTheCastWithoutRetrying() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking { kitchen.attach() }
        ma.fail = MaException(23, "Invalid or expired token")
        runBlocking { kitchen.load(media(), 0, play = true) }
        assertEquals(RemoteState.GONE, kitchen.status.value.state)
        assertEquals(1, ma.calls.count { it.startsWith("player_queues/play_media") })
    }

    @Test fun closeStopsAndPutsAutoplayBack() {
        val ma = FakeMa()
        val kitchen = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma)
        runBlocking { kitchen.attach() }
        kitchen.close()
        val deadline = System.currentTimeMillis() + 3_000
        while (System.currentTimeMillis() < deadline && ma.released == 0) Thread.sleep(20)
        assertEquals(1, ma.released)
        assertTrue(ma.calls.any { it.startsWith("player_queues/stop") })
        // The queue had it on before; it's turned back on.
        assertTrue(ma.calls.last { it.startsWith("player_queues/dont_stop_the_music") }.endsWith("\"dont_stop_the_music_enabled\":true}"))
    }

    @Test fun picksCastablePlayers() {
        val players = MaPlayers.castable(JSONArray(PLAYERS))
        assertEquals(listOf("media_player.kitchen", "downstairs"), players.map { it.getString("player_id") })
        val group = MaPlayers.target(players[1], "ws://homeassistant.local:8095/ws", "srv1")
        assertEquals(CastTarget.Kind.MUSIC_ASSISTANT, group.kind)
        assertEquals("ma:srv1:downstairs", group.id)
        assertEquals("downstairs", group.ma!!.queueId)
        assertEquals("homeassistant.local", group.host)
        assertTrue(MaPlayers.sameName("Kitchen Echo Show", "kitchen echo-show"))
        assertFalse(MaPlayers.sameName("Kitchen", "Lounge"))
    }

    @Test fun readsServerAddresses() {
        assertEquals("ws://homeassistant.local:8095/ws", MaClient.wsUrl("homeassistant.local"))
        assertEquals("ws://192.168.1.5:8095/ws", MaClient.wsUrl("http://192.168.1.5:8095/"))
        assertEquals("ws://192.168.1.5:9000/ws", MaClient.wsUrl("192.168.1.5:9000"))
        assertEquals("wss://ma.example.com/ws", MaClient.wsUrl("https://ma.example.com"))
        assertEquals("ws://10.0.0.2:8095/ws", MaClient.wsUrl("ws://10.0.0.2:8095/ws"))
    }

    @Test fun matchesQueueItemsToWhatWasSent() {
        assertTrue(MusicAssistant.sameMedia("ytmusic://track/abc123", "ytmusic://track/abc123"))
        assertTrue(MusicAssistant.sameMedia("ytmusic--x7Qk://track/abc123", "ytmusic://track/abc123"))
        assertFalse(MusicAssistant.sameMedia("ytmusic://track/zzz", "ytmusic://track/abc123"))
        val url = "http://192.168.1.20:41234/s/4f1c2a9e.m4a"
        assertTrue(MusicAssistant.sameMedia(url, url))
        assertTrue(MusicAssistant.sameMedia("builtin://track/" + java.net.URLEncoder.encode(url, "UTF-8"), url))
        assertNull(MusicAssistant.uriOf(JSONObject("""{"queue_item_id":"x","name":"y"}""")))
        assertEquals(true, MusicAssistant.autoplayOf(JSONObject("""{"autoplay_enabled":true}""")))
    }

    /**
     * A pretend Music Assistant server on a real WebSocket: says who it is, answers `auth`, and
     * sends `players/all` in two partial pieces. The `auth` reply arrives while the client is still
     * signing in, which once had it taken for a second, schema-less server greeting.
     */
    private class PretendServer(private val goodToken: String) : AutoCloseable {
        private val server = java.net.ServerSocket(0)
        val connections = java.util.concurrent.atomic.AtomicInteger()
        val commands: MutableList<String> = Collections.synchronizedList(mutableListOf<String>())
        val url get() = "ws://127.0.0.1:${server.localPort}/ws"

        init {
            kotlin.concurrent.thread(isDaemon = true) {
                while (!server.isClosed) {
                    val s = runCatching { server.accept() }.getOrNull() ?: break
                    connections.incrementAndGet()
                    kotlin.concurrent.thread(isDaemon = true) { runCatching { serve(s) }; runCatching { s.close() } }
                }
            }
        }

        private fun serve(s: java.net.Socket) {
            val input = java.io.DataInputStream(s.getInputStream())
            val output = s.getOutputStream()
            var key = ""
            while (true) {
                val line = readLine(input)
                if (line.isEmpty()) break
                if (line.startsWith("Sec-WebSocket-Key:", true)) key = line.substringAfter(':').trim()
            }
            val accept = java.util.Base64.getEncoder().encodeToString(
                java.security.MessageDigest.getInstance("SHA-1").digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray()),
            )
            output.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\n\r\n".toByteArray())
            fun send(text: String) = synchronized(output) {
                val bytes = text.toByteArray()
                output.write(0x81)
                if (bytes.size < 126) output.write(bytes.size) else { output.write(126); output.write(bytes.size shr 8); output.write(bytes.size and 0xff) }
                output.write(bytes)
                output.flush()
            }
            send("""{"server_id":"srv1","server_version":"2.10.12","schema_version":65,"min_supported_schema_version":28,"homeassistant_addon":true,"onboard_done":true}""")
            while (true) {
                val head = input.readUnsignedByte()
                var length = (input.readUnsignedByte() and 0x7f).toLong()
                if (length == 126L) length = input.readUnsignedShort().toLong() else if (length == 127L) length = input.readLong()
                val mask = ByteArray(4).also(input::readFully)
                val payload = ByteArray(length.toInt()).also(input::readFully).also { p -> for (i in p.indices) p[i] = (p[i].toInt() xor mask[i % 4].toInt()).toByte() }
                when (head and 0x0f) {
                    8 -> return
                    1 -> Unit
                    else -> continue
                }
                val json = JSONObject(String(payload))
                val id = json.getString("message_id")
                val command = json.getString("command")
                commands += command
                when (command) {
                    "auth" -> if (json.getJSONObject("args").optString("token") == goodToken) {
                        send("""{"message_id":"$id","result":{"authenticated":true,"user":{"username":"owner"}}}""")
                    } else {
                        send("""{"message_id":"$id","error_code":23,"details":"Invalid or expired token"}""")
                    }
                    "players/all" -> {
                        send("""{"message_id":"$id","result":[{"player_id":"a","name":"Kitchen"}],"partial":true}""")
                        send("""{"message_id":"$id","result":[{"player_id":"b","name":"Lounge"}]}""")
                    }
                    else -> send("""{"message_id":"$id","error_code":12,"details":"Invalid command: $command"}""")
                }
            }
        }

        private fun readLine(input: java.io.DataInputStream): String {
            val line = StringBuilder()
            while (true) {
                val c = input.readUnsignedByte()
                if (c == '\n'.code) return line.toString().trimEnd('\r')
                line.append(c.toChar())
            }
        }

        override fun close() = server.close()
    }

    @Test fun signsInAndReadsAPartialList() {
        PretendServer("good-token").use { server ->
            val client = MaClient(server.url, { "good-token" }, okhttp3.OkHttpClient())
            client.retain()
            try {
                val info = runBlocking { client.connect() }
                assertEquals("2.10.12", info.version)
                assertEquals(65, info.schema)
                val players = runBlocking { client.call("players/all") } as JSONArray
                assertEquals(listOf("a", "b"), (0 until players.length()).map { players.getJSONObject(it).getString("player_id") })
                assertEquals(listOf("auth", "players/all"), server.commands.toList())
                // Asked again, the same connection answers.
                runBlocking { client.connect() }
                assertEquals(1, server.connections.get())
            } finally {
                client.release()
            }
        }
    }

    @Test fun aWrongTokenIsRefusedOnceAndNotRetried() {
        PretendServer("good-token").use { server ->
            val client = MaClient(server.url, { "revoked" }, okhttp3.OkHttpClient())
            client.retain()
            try {
                val first = runCatching { runBlocking { client.connect() } }.exceptionOrNull()
                assertTrue(first is MaException && first.signIn)
                val again = runCatching { runBlocking { client.call("players/all") } }.exceptionOrNull()
                assertTrue(again is MaException && again.signIn)
                Thread.sleep(1_500)
                // Turned down once: no second connection, and nothing sent after the refusal.
                assertEquals(1, server.connections.get())
                assertEquals(listOf("auth"), server.commands.toList())
                assertEquals("Invalid or expired token", client.problem.value)
            } finally {
                client.release()
            }
        }
    }

    private fun queueEvent(queue: String, state: String, item: String, uri: String, elapsed: Double, ended: Boolean = false) = JSONObject()
        .put("event", "queue_updated")
        .put("object_id", queue)
        .put("data", JSONObject()
            .put("queue_id", queue)
            .put("state", state)
            .put("ended", ended)
            .put("elapsed_time", elapsed)
            .put("elapsed_time_last_updated", System.currentTimeMillis() / 1000.0)
            .put("current_item", JSONObject().put("queue_item_id", item).put("name", "Song").put("duration", 200)
                .put("media_item", JSONObject().put("uri", uri))))
        .toString()

    private companion object {
        /** Shaped as `player_queues/all` answers, cut down. */
        const val QUEUES = """[
            {"queue_id":"media_player.kitchen","display_name":"Kitchen Echo Show","state":"idle","dont_stop_the_music_enabled":true,
             "current_item":{"queue_item_id":"old-item","name":"Old","media_item":{"uri":"ytmusic://track/oldSong"}},"elapsed_time":0},
            {"queue_id":"media_player.lounge","display_name":"Lounge","state":"idle","dont_stop_the_music_enabled":false,"current_item":null}
        ]"""

        /** Shaped as `players/all` answers, cut down. */
        const val PLAYERS = """[
            {"player_id":"media_player.kitchen","name":"Kitchen Echo Show","type":"player","available":true,"enabled":true,"hide_in_ui":false,
             "playback_state":"idle","volume_level":40,"provider":"hass","device_info":{"model":"Echo Show 8"},"group_members":[],"synced_to":null},
            {"player_id":"hidden_one","name":"Hidden","type":"player","available":true,"enabled":true,"hide_in_ui":true},
            {"player_id":"offline","name":"Offline","type":"player","available":false,"enabled":true},
            {"player_id":"downstairs","name":"Downstairs","type":"group","available":true,"enabled":true,"group_members":["a","b"]},
            {"player_id":"a","name":"Member A","type":"player","available":true,"enabled":true,"synced_to":"downstairs"}
        ]"""
    }
}
