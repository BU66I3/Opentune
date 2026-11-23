package com.opentune.cast

import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import androidx.media3.common.Player
import com.opentune.playback.MusicAssistantPlayer
import java.util.Collections
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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
import org.robolectric.shadows.ShadowLooper

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = android.app.Application::class)
class MusicAssistantTest {
    /** A pretend server: records each command and answers the reads from fixed JSON. */
    private class FakeMa(
        @Volatile var queues: String = QUEUES,
        private val players: String = PLAYERS,
        @Volatile var items: String = "[]",
    ) : MaApi {
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
                "player_queues/items" -> JSONArray(items)
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

        fun sent(command: String) = calls.filter { it.startsWith("$command ") }
    }

    private fun media(id: String = "dQw4w9WgXcQ", backup: String? = null) =
        CastMedia(id, MusicAssistant.youTubeUri(id), "audio/mp4", "T", "A", null, null, 200_000, fallbackUrl = backup)

    /** Waits for [check], since commands and events are handled on other threads. */
    private fun waitUntil(ms: Long = 3_000, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + ms
        while (System.currentTimeMillis() < deadline && !check()) {
            ShadowLooper.idleMainLooper()
            Thread.sleep(20)
        }
        ShadowLooper.idleMainLooper()
    }

    private fun kitchen(ma: FakeMa) = MusicAssistant("Kitchen", "media_player.kitchen", "media_player.kitchen", ma).also { runBlocking { it.attach() } }

    /** The phone's queue goes over in one hand-off: the first song alone, so it starts at once, then the rest in batches. */
    @Test fun handsOverTheQueueInOneGo() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        val songs = (1..60).map { media("song%07d".format(it)) }
        runBlocking { kitchen.handOff(songs, 0) }
        val plays = ma.sent("player_queues/play_media").map { JSONObject(it.substringAfter(' ')) }
        assertEquals(listOf("replace", "add", "add", "add"), plays.map { it.getString("option") })
        assertEquals(listOf(1, 25, 25, 9), plays.map { it.getJSONArray("media").length() })
        assertEquals("ytmusic://track/song0000001", plays[0].getJSONArray("media").getString(0))
        assertEquals("ytmusic://track/song0000060", plays[3].getJSONArray("media").getString(8))
        // Started from the top: nothing to seek.
        Thread.sleep(200)
        assertTrue(ma.sent("player_queues/seek").isEmpty())
        // Music Assistant's own autoplay is left as the owner has it.
        assertTrue(ma.calls.none { it.startsWith("player_queues/dont_stop_the_music") })
    }

    @Test fun aSongFromThePhoneGoesWithItsDetails() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        val url = "http://192.168.1.20:41234/s/a2c1e1862f4e.m4a"
        val song = CastMedia("song", url, "audio/mp4", "Night Swim", "Bonobo", "Fragments", "https://lh3.example/cover.jpg", 194_000)
        runBlocking { kitchen.handOff(listOf(song), 0) }
        val sent = JSONObject(ma.sent("player_queues/play_media").single().substringAfter(' '))
        val track = sent.getJSONArray("media").getJSONObject(0)
        assertEquals("Night Swim", track.getString("name"))
        assertEquals("track", track.getString("media_type"))
        assertEquals(url, track.getString("item_id"))
        assertEquals("builtin", track.getJSONArray("provider_mappings").getJSONObject(0).getString("provider_domain"))
        assertEquals("Bonobo", track.getJSONArray("artists").getJSONObject(0).getString("name"))
        assertEquals("Fragments", track.getJSONObject("album").getString("name"))
        assertEquals(194, track.getInt("duration"))
        assertEquals("https://lh3.example/cover.jpg", track.getJSONObject("metadata").getJSONArray("images").getJSONObject(0).getString("path"))
    }

    @Test fun aServerThatWontTakeDetailsGetsTheAddress() {
        val ma = FakeMa()
        ma.refuse = { line -> if (line.startsWith("player_queues/play_media") && "\"name\"" in line) MaException(3, "Invalid data") else null }
        val kitchen = kitchen(ma)
        val url = "http://192.168.1.20:41234/s/a2c1e1862f4e.m4a"
        val song = CastMedia("song", url, "audio/mp4", "Night Swim", "Bonobo", null, "https://lh3.example/cover.jpg", 194_000)
        runBlocking { kitchen.handOff(listOf(song, song.copy(id = "next")), 0) }
        val plays = ma.sent("player_queues/play_media")
        // Turned down once with its details, then the address alone, and from then on the address alone.
        assertEquals(3, plays.size)
        assertTrue("\"name\"" in plays[0])
        assertTrue(plays[1].endsWith(""""media":["$url"],"option":"replace"}"""))
        assertTrue(plays[2].endsWith(""""media":["$url"],"option":"add"}"""))
    }

    /** MA lists a YouTube Music provider that can't play the songs: they come through the phone instead. */
    @Test fun aFailingYouTubeMusicFallsBackToThePhone() {
        val ma = FakeMa()
        ma.refuse = { line -> if ("ytmusic://" in line) MaException(8, "There is nothing to play here.") else null }
        val kitchen = kitchen(ma)
        val songs = (1..3).map { media("song%07d".format(it), backup = "http://192.168.1.20:41234/s/backup$it.m4a").copy(title = "Song $it") }
        runBlocking { kitchen.handOff(songs, 0) }
        val plays = ma.sent("player_queues/play_media").map { JSONObject(it.substringAfter(' ')) }
        // YouTube Music tried for the first, then the phone's addresses for it and the rest.
        assertEquals(3, plays.size)
        assertEquals("ytmusic://track/song0000001", plays[0].getJSONArray("media").getString(0))
        assertEquals("http://192.168.1.20:41234/s/backup1.m4a", plays[1].getJSONArray("media").getJSONObject(0).getString("item_id"))
        assertEquals("Song 1", plays[1].getJSONArray("media").getJSONObject(0).getString("name"))
        assertEquals("add", plays[2].getString("option"))
        assertEquals(2, plays[2].getJSONArray("media").length())
        assertNull(runBlocking { kitchen.youTubeMusic() })
    }

    @Test fun followsTheQueue() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic--x7Qk://track/dQw4w9WgXcQ", 3.0, index = 2, shuffle = true, repeat = "all"))
        waitUntil { kitchen.queue.value.itemId == "new-item" && kitchen.status.value.state == RemoteState.PLAYING }
        val q = kitchen.queue.value
        assertEquals("playing", q.state)
        assertEquals(2, q.index)
        assertTrue(q.shuffle)
        assertEquals("all", q.repeat)
        assertTrue(q.positionNow() in 3_000L..4_000L)
        assertEquals(RemoteState.PLAYING, kitchen.status.value.state)
        // Another queue's events are none of its business.
        ma.replay(queueEvent("media_player.lounge", "paused", "x", "ytmusic://track/other", 1.0))
        ma.replay("""{"event":"queue_time_updated","object_id":"media_player.lounge","data":99}""")
        ma.replay("""{"event":"queue_time_updated","object_id":"media_player.kitchen","data":61.5}""")
        waitUntil { kitchen.queue.value.positionMs == 61_500L }
        assertEquals("new-item", kitchen.queue.value.itemId)
        assertEquals(61_500L, kitchen.queue.value.positionMs)
        // Stopped (an Echo Show stops rather than pauses): 0:00 reported, held where it was.
        ma.replay(queueEvent("media_player.kitchen", "idle", "new-item", "ytmusic://track/dQw4w9WgXcQ", 0.0))
        waitUntil { kitchen.queue.value.state == "idle" }
        assertTrue(kitchen.queue.value.positionMs in 61_500L..63_000L)
        ma.replay("""{"event":"player_updated","object_id":"media_player.kitchen","data":{"player_id":"media_player.kitchen","available":true,"volume_level":35}}""")
        waitUntil { kitchen.status.value.volume == 0.35f }
        assertEquals(0.35f, kitchen.status.value.volume!!, 0.001f)
    }

    @Test fun readsTheQueuesSongs() {
        val ma = FakeMa(items = ITEMS)
        val kitchen = kitchen(ma)
        val items = kitchen.items.value
        assertEquals(listOf("i1", "i2", "i3"), items.map { it.id })
        assertEquals("cccccccccc3", items[2].youTubeId)
        assertEquals("Third", items[2].title)
        assertEquals("Bonobo, Totally Enormous Extinct Dinosaurs", items[2].artist)
        assertEquals("Fragments", items[2].album)
        assertEquals("https://img.example/i3.jpg", items[2].imageUrl)
        assertEquals(185_000L, items[2].durationMs)
        assertNull(items[1].youTubeId)
        // The songs changed on the speaker: read again.
        ma.items = JSONArray(ITEMS).apply { remove(1) }.toString()
        ma.replay("""{"event":"queue_items_updated","object_id":"media_player.kitchen","data":{"queue_id":"media_player.kitchen","state":"playing","items":2}}""")
        waitUntil { kitchen.items.value.size == 2 }
        assertEquals(listOf("i1", "i3"), kitchen.items.value.map { it.id })
        assertNull(MusicAssistant.youTubeIdOf("ytmusic://track/short"))
        assertEquals("dQw4w9WgXcQ", MusicAssistant.youTubeIdOf("ytmusic_free--C2P9fad2://track/dQw4w9WgXcQ"))
    }

    /** MA turns a seek down while the song handed over is starting: it's tried again once it's under way. */
    @Test fun aHandOffStartsWhereThePhoneWas() {
        val ma = FakeMa()
        var refusals = 1
        ma.refuse = { line -> if (line.startsWith("player_queues/seek") && refusals-- > 0) MaException(12, "Invalid or unsupported command.") else null }
        val kitchen = kitchen(ma)
        runBlocking { kitchen.handOff(listOf(media()), 42_000) }
        ma.replay(queueEvent("media_player.kitchen", "playing", "new-item", "ytmusic://track/dQw4w9WgXcQ", 0.0))
        waitUntil(5_000) { ma.sent("player_queues/seek").size >= 2 }
        val seeks = ma.sent("player_queues/seek")
        assertEquals(2, seeks.size)
        assertTrue(seeks.all { "\"position\":4" in it })
        // A later seek still goes: the refusal was only for the moment.
        runBlocking { kitchen.seek(90_000) }
        assertTrue(ma.sent("player_queues/seek").last().contains("\"position\":90"))
    }

    /** A player that turns seeking down as unsupported is asked once; no more seeks are sent to it. */
    @Test fun aPlayerThatCantSeekIsAskedOnce() {
        val ma = FakeMa()
        ma.refuse = { line -> if (line.startsWith("player_queues/seek")) MaException(9, "Unsupported feature") else null }
        val kitchen = kitchen(ma)
        runBlocking {
            kitchen.seek(30_000)
            kitchen.seek(60_000)
        }
        assertEquals(1, ma.sent("player_queues/seek").size)
    }

    @Test fun commandsGoToTheQueue() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        ma.calls.clear()
        runBlocking {
            kitchen.play()
            kitchen.pause()
            kitchen.next()
            kitchen.previous()
            kitchen.playIndex(3, 30_000)
            kitchen.remove("i2")
            kitchen.move("i3", -1)
            kitchen.shuffle(true)
            kitchen.repeat("one")
            kitchen.setVolume(0.62f)
            kitchen.add(listOf(media("aaaaaaaaaa1"), media("bbbbbbbbbb2")), next = true)
        }
        assertEquals(
            listOf(
                """player_queues/play {"queue_id":"media_player.kitchen"}""",
                """player_queues/pause {"queue_id":"media_player.kitchen"}""",
                """player_queues/next {"queue_id":"media_player.kitchen"}""",
                """player_queues/previous {"queue_id":"media_player.kitchen"}""",
                """player_queues/play_index {"index":3,"seek_position":30,"queue_id":"media_player.kitchen"}""",
                """player_queues/delete_item {"item_id_or_index":"i2","queue_id":"media_player.kitchen"}""",
                """player_queues/move_item {"queue_item_id":"i3","pos_shift":-1,"queue_id":"media_player.kitchen"}""",
                """player_queues/shuffle {"shuffle_enabled":true,"queue_id":"media_player.kitchen"}""",
                """player_queues/repeat {"repeat_mode":"one","queue_id":"media_player.kitchen"}""",
                """players/cmd/volume_set {"player_id":"media_player.kitchen","volume_level":62}""",
                """player_queues/play_media {"queue_id":"media_player.kitchen","media":["ytmusic://track/aaaaaaaaaa1","ytmusic://track/bbbbbbbbbb2"],"option":"next"}""",
            ),
            ma.calls.toList(),
        )
    }

    @Test fun aPlayerTakenAwayIsGone() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        ma.replay("""{"event":"player_removed","object_id":"media_player.kitchen","data":null}""")
        waitUntil { kitchen.status.value.state == RemoteState.GONE }
        assertEquals(RemoteState.GONE, kitchen.status.value.state)
    }

    @Test fun aRefusedTokenEndsTheCastWithoutRetrying() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        ma.fail = MaException(23, "Invalid or expired token")
        runBlocking { kitchen.handOff(listOf(media(), media("bbbbbbbbbb2")), 0) }
        assertEquals(RemoteState.GONE, kitchen.status.value.state)
        assertEquals(1, ma.sent("player_queues/play_media").size)
    }

    @Test fun closeStopsIt() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        kitchen.close()
        waitUntil { ma.released > 0 }
        assertEquals(1, ma.released)
        assertTrue(ma.calls.any { it.startsWith("player_queues/stop") })
    }

    // ---- The session's player while casting ----------------------------------------------

    private fun phoneSong(id: String) = MediaItem.Builder()
        .setMediaId(id)
        .setUri("https://example.com/$id")
        .setMediaMetadata(MediaMetadata.Builder().setTitle("Song $id").setArtist("Artist").build())
        .build()

    private fun player(kitchen: MusicAssistant, scope: CoroutineScope) = MusicAssistantPlayer(kitchen, scope) { item, _ ->
        CastMedia(item.mediaId, MusicAssistant.youTubeUri(item.mediaId), "audio/mp4", item.mediaMetadata.title.toString(), "Artist", null, null, 0)
    }

    /** Cast to a speaker already playing: the player shows its queue, where it is, and drives it. */
    @Test fun thePlayerShowsAndDrivesTheSpeakersQueue() {
        val ma = FakeMa(queues = playing(), items = ITEMS)
        val kitchen = kitchen(ma)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        try {
            val player = player(kitchen, scope)
            waitUntil { player.mediaItemCount == 3 }
            assertEquals(3, player.mediaItemCount)
            assertEquals(2, player.currentMediaItemIndex)
            assertEquals("cccccccccc3", player.currentMediaItem!!.mediaId)
            assertEquals("Third", player.currentMediaItem!!.mediaMetadata.title.toString())
            assertTrue(player.playWhenReady)
            assertTrue(player.currentPosition in 61_000L..64_000L)
            assertEquals(185_000L, player.duration)
            assertEquals(40, player.deviceVolume)
            // Paused on the phone: shown at once, and sent.
            ma.calls.clear()
            player.pause()
            assertFalse(player.playWhenReady)
            waitUntil { ma.sent("player_queues/pause").isNotEmpty() }
            assertEquals(1, ma.sent("player_queues/pause").size)
            // Back to the first song, from the phone's queue screen.
            player.seekTo(0, 0)
            waitUntil { ma.sent("player_queues/play_index").isNotEmpty() }
            assertTrue(ma.sent("player_queues/play_index").single().contains("\"index\":0"))
            assertEquals(0, player.currentMediaItemIndex)
            // The phone's Next goes to the speaker's queue.
            player.seekTo(2, 0)
            player.seekToNextMediaItem()
            player.seekToPrevious()
            waitUntil { ma.sent("player_queues/previous").isNotEmpty() }
            assertTrue(ma.calls.any { it.startsWith("player_queues/play_index") && "\"index\":2" in it })
            // The volume keys move the speaker's volume.
            player.setDeviceVolume(70, 0)
            waitUntil { ma.sent("players/cmd/volume_set").isNotEmpty() }
            assertTrue(ma.sent("players/cmd/volume_set").single().contains("\"volume_level\":70"))
            player.release()
        } finally {
            scope.cancel()
        }
    }

    /** Cast while the phone is paused: its songs show, paused, and go over only when played. */
    @Test fun aPausedPhonesQueueGoesOverWhenPlayed() {
        val ma = FakeMa()
        val kitchen = kitchen(ma)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        try {
            val player = player(kitchen, scope)
            val songs = listOf("aaaaaaaaaa1", "bbbbbbbbbb2", "cccccccccc3").map(::phoneSong)
            player.stage(songs, 1, 30_000)
            ShadowLooper.idleMainLooper()
            assertEquals(3, player.mediaItemCount)
            assertEquals("bbbbbbbbbb2", player.currentMediaItem!!.mediaId)
            assertFalse(player.playWhenReady)
            Thread.sleep(200)
            assertTrue(ma.sent("player_queues/play_media").isEmpty())
            // Played: the phone's songs from the current one go to the speaker.
            player.play()
            waitUntil { ma.sent("player_queues/play_media").size >= 2 }
            val plays = ma.sent("player_queues/play_media").map { JSONObject(it.substringAfter(' ')) }
            assertEquals("ytmusic://track/bbbbbbbbbb2", plays[0].getJSONArray("media").getString(0))
            assertEquals("replace", plays[0].getString("option"))
            assertEquals("ytmusic://track/cccccccccc3", plays[1].getJSONArray("media").getString(0))
            assertTrue(player.playWhenReady)
            // The speaker takes them up: its queue is shown, the phone's songs as the phone had them.
            ma.items = """[
                {"queue_item_id":"n2","name":"x","duration":200,"media_item":{"uri":"ytmusic://track/bbbbbbbbbb2","name":"x"}},
                {"queue_item_id":"n3","name":"y","duration":200,"media_item":{"uri":"ytmusic://track/cccccccccc3","name":"y"}}
            ]"""
            ma.replay(queueEvent("media_player.kitchen", "playing", "n2", "ytmusic://track/bbbbbbbbbb2", 0.5, index = 0, items = 2))
            waitUntil { player.mediaItemCount == 2 }
            assertEquals(2, player.mediaItemCount)
            assertEquals("Song bbbbbbbbbb2", player.currentMediaItem!!.mediaMetadata.title.toString())
            assertEquals(Player.STATE_READY, player.playbackState)
            player.release()
        } finally {
            scope.cancel()
        }
    }

    /**
     * A server with a YouTube Music provider other than the official one (a free one,
     * `ytmusic_free`) gets songs addressed to it: sent as `ytmusic://`, MA looked for the
     * official one, and every song failed and came through the phone.
     */
    @Test fun songsGoToTheYouTubeMusicProviderTheServerHas() {
        fun p(domain: String, instance: String = domain, available: Boolean = true) =
            JSONObject().put("domain", domain).put("instance_id", instance).put("available", available)
        assertEquals("ytmusic_free", MusicAssistant.youTubeMusicOf(listOf(p("spotify"), p("ytmusic_free"))))
        // The official one first, when both are working.
        assertEquals("ytmusic", MusicAssistant.youTubeMusicOf(listOf(p("ytmusic_free"), p("ytmusic"))))
        // One that isn't working is passed over.
        assertEquals("ytmusic_free", MusicAssistant.youTubeMusicOf(listOf(p("ytmusic", available = false), p("ytmusic_free"))))
        // The instance is named, so a second copy of a provider gets the song too.
        assertEquals("ytmusic--a1b2", MusicAssistant.youTubeMusicOf(listOf(p("ytmusic", "ytmusic--a1b2"))))
        assertNull(MusicAssistant.youTubeMusicOf(listOf(p("spotify"), p("ytmusic", available = false))))
        assertEquals("ytmusic_free://track/dQw4w9WgXcQ", MusicAssistant.youTubeUri("dQw4w9WgXcQ", "ytmusic_free"))
        // What it plays is known as the song sent.
        assertTrue(MusicAssistant.sameMedia("ytmusic_free://track/dQw4w9WgXcQ", "ytmusic_free://track/dQw4w9WgXcQ"))
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
                // Let go and taken up again soon after (the scan, then a speaker picked): still the same one.
                client.release()
                client.retain()
                runBlocking { client.call("players/all") }
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

    private fun queueEvent(
        queue: String,
        state: String,
        item: String,
        uri: String,
        elapsed: Double,
        index: Int = 0,
        shuffle: Boolean = false,
        repeat: String = "off",
        items: Int? = null,
    ) = JSONObject()
        .put("event", "queue_updated")
        .put("object_id", queue)
        .put("data", JSONObject()
            .put("queue_id", queue)
            .put("state", state)
            .put("current_index", index)
            .put("shuffle_enabled", shuffle)
            .put("repeat_mode", repeat)
            .put("elapsed_time", elapsed)
            .put("elapsed_time_last_updated", System.currentTimeMillis() / 1000.0)
            .apply { if (items != null) put("items", items) }
            .put("current_item", JSONObject().put("queue_item_id", item).put("name", "Song").put("duration", 200)
                .put("media_item", JSONObject().put("uri", uri))))
        .toString()

    private companion object {
        /** A queue playing the third of [ITEMS], a minute in as of now. */
        fun playing() = """[
            {"queue_id":"media_player.kitchen","state":"playing","current_index":2,"items":3,"elapsed_time":61.0,
             "elapsed_time_last_updated":${System.currentTimeMillis() / 1000.0},
             "current_item":{"queue_item_id":"i3","name":"Third","media_item":{"uri":"ytmusic_free--C2P9fad2://track/cccccccccc3"}}}
        ]"""

        /** Shaped as `player_queues/items` answers: two YouTube Music songs and one from elsewhere. */
        const val ITEMS = """[
            {"queue_item_id":"i1","name":"First","duration":200,"media_item":{"uri":"ytmusic://track/aaaaaaaaaa1","name":"First"}},
            {"queue_item_id":"i2","name":"Other","duration":200,"media_item":{"uri":"spotify://track/4uLU6hMCjMI75M1A2tKUQC","name":"Other"}},
            {"queue_item_id":"i3","name":"Third","duration":185,"image":{"path":"https://img.example/i3.jpg"},
             "media_item":{"uri":"ytmusic_free--C2P9fad2://track/cccccccccc3","name":"Third",
              "artists":[{"name":"Bonobo"},{"name":"Totally Enormous Extinct Dinosaurs"}],"album":{"name":"Fragments"}}}
        ]"""

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
