package com.opentune.cast

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = android.app.Application::class)
class RecentCastsTest {
    private val chromecast = CastTarget("cc-1", "Living Room TV", CastTarget.Kind.CHROMECAST, "192.168.1.30", 8009, "BRAVIA")
    private val dlna = CastTarget(
        "uuid:1234", "Kitchen Speaker", CastTarget.Kind.DLNA, "192.168.1.20",
        dlna = DlnaDevice("Kitchen Speaker", "http://192.168.1.20:1400/avt", null, "uuid:1234"),
    )
    private val echo = CastTarget(
        "ma:srv1:up412798a9", "Office - Echo Show 5", CastTarget.Kind.MUSIC_ASSISTANT, "192.168.1.50",
        ma = MaPlayerRef("ws://192.168.1.50:8095/ws", "srv1", "up412798a9", "up412798a9", null),
    )

    @Test fun keepsEveryKindOfDevice() {
        val back = RecentCasts.fromJson(RecentCasts.toJson(listOf(echo, chromecast, dlna)))
        assertEquals(listOf(echo, chromecast, dlna), back)
    }

    @Test fun newestFirstOnceEachAndNoMoreThanThree() {
        listOf(chromecast, dlna, echo).forEach { RecentCasts.forget(it.id) }
        RecentCasts.remember(chromecast)
        RecentCasts.remember(dlna)
        RecentCasts.remember(chromecast)
        assertEquals(listOf("cc-1", "uuid:1234"), RecentCasts.list.value.map { it.id })
        RecentCasts.remember(echo)
        RecentCasts.remember(CastTarget("cc-2", "Bedroom", CastTarget.Kind.CHROMECAST, "192.168.1.31"))
        assertEquals(listOf("cc-2", "ma:srv1:up412798a9", "cc-1"), RecentCasts.list.value.map { it.id })
    }

    @Test fun leavesOutWhatItCantRead() {
        val text = """[
            {"id":"x","name":"Future thing","kind":"AIRPLAY","host":"h","port":0},
            {"id":"y","name":"DLNA without its details","kind":"DLNA","host":"h","port":0},
            ${RecentCasts.toJson(listOf(chromecast)).removePrefix("[").removeSuffix("]")}
        ]"""
        assertEquals(listOf(chromecast), RecentCasts.fromJson(text))
    }
}
