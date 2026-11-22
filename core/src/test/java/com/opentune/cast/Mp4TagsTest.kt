package com.opentune.cast

import androidx.test.core.app.ApplicationProvider
import java.io.ByteArrayOutputStream
import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = android.app.Application::class)
class Mp4TagsTest {
    private val tags = Mp4Tags.Tags("Night Swim", "Bonobo", "Fragments")

    @Test fun tagsAFragmentedStream() {
        val file = stream()
        val tagged = Mp4Tags.tag(file, tags)
        assertNotNull(tagged)
        val moovEnd = MOOV_START + moovSize(file)
        assertEquals(moovEnd, tagged!!.replaced)
        // The same bytes up to moov, a moov grown by exactly the udta added, then nothing else.
        val udta = Mp4Tags.udta(tags)
        assertEquals(moovEnd + udta.size, tagged.header.size)
        assertEquals(moovSize(file) + udta.size, moovSize(tagged.header))
        assertArrayEquals(file.copyOfRange(0, MOOV_START), tagged.header.copyOfRange(0, MOOV_START))
        val text = String(tagged.header, Charsets.ISO_8859_1)
        assertTrue("©namNight Swim" !in text) // inside a data atom, not straight after the code
        assertTrue("udta" in text && "meta" in text && "mdir" in text && "ilst" in text)
        assertTrue(String(tagged.header, Charsets.UTF_8).contains("Night Swim"))
        assertTrue(String(tagged.header, Charsets.UTF_8).contains("Bonobo"))
        assertTrue(String(tagged.header, Charsets.UTF_8).contains("Fragments"))
    }

    /** Some of YouTube's files carry a udta of their own, with no title; ours takes its place. */
    @Test fun replacesAUdtaAlreadyThere() {
        val file = stream(tagged = true)
        val tagged = Mp4Tags.tag(file, tags)
        assertNotNull(tagged)
        val udta = Mp4Tags.udta(tags)
        // The old udta (12 bytes) out, the new one in.
        assertEquals(moovSize(file) - 12 + udta.size, moovSize(tagged!!.header))
        assertEquals(1, Regex("udta").findAll(String(tagged.header, Charsets.ISO_8859_1)).count())
        assertTrue(String(tagged.header, Charsets.UTF_8).contains("Night Swim"))
        // Everything else in moov is kept, in order, ahead of the new udta.
        val text = String(tagged.header, Charsets.ISO_8859_1)
        assertTrue(text.indexOf("mvhd") < text.indexOf("trak") && text.indexOf("trak") < text.indexOf("mvex") && text.indexOf("mvex") < text.indexOf("udta"))
    }

    @Test fun leavesAloneWhatItCantSafelyChange() {
        // Not fragmented: no mvex in moov.
        assertNull(Mp4Tags.tag(stream(fragmented = false), tags))
        // A fragment placing its data from the file's start.
        assertNull(Mp4Tags.tag(stream(absolute = true), tags))
        // Cut off before the first fragment.
        assertNull(Mp4Tags.tag(stream().copyOfRange(0, MOOV_START + 20), tags))
        assertNull(Mp4Tags.tag(ByteArray(100) { 7 }, tags))
    }

    /** The phone's server passes a stream through with the tagged header in front, ranges included. */
    @Test fun servesATaggedStream() {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        CastServer.start(context)
        try {
            val original = stream(mdatBytes = 200_000)
            val file = File.createTempFile("ytaudio", ".m4a").apply { writeBytes(original); deleteOnExit() }
            val loopback = InetAddress.getLoopbackAddress()
            val upstream = CastServer.publish(CastServer.Source.OnPhone(file, "audio/mp4"), loopback)
            val url = CastServer.publish(CastServer.Source.Web("audio/mp4", tags) { upstream to emptyMap() }, loopback)
            val tagged = Mp4Tags.tag(original, tags)!!
            val expected = tagged.header + original.copyOfRange(tagged.replaced, original.size)
            val http = OkHttpClient()
            http.newCall(Request.Builder().url(url).build()).execute().use {
                assertEquals(200, it.code)
                assertEquals("${expected.size}", it.header("Content-Length"))
                assertArrayEquals(expected, it.body!!.bytes())
            }
            // A range across the end of the new header.
            val from = tagged.header.size - 10
            val to = tagged.header.size + 99
            http.newCall(Request.Builder().url(url).header("Range", "bytes=$from-$to").build()).execute().use {
                assertEquals(206, it.code)
                assertEquals("bytes $from-$to/${expected.size}", it.header("Content-Range"))
                assertArrayEquals(expected.copyOfRange(from, to + 1), it.body!!.bytes())
            }
            // A range across the end of what was read to tag it (held in memory) and the rest (fetched).
            val edge = 64 * 1024 - tagged.replaced + tagged.header.size
            http.newCall(Request.Builder().url(url).header("Range", "bytes=${edge - 500}-${edge + 499}").build()).execute().use {
                assertArrayEquals(expected.copyOfRange(edge - 500, edge + 500), it.body!!.bytes())
            }
            // A range entirely past it.
            http.newCall(Request.Builder().url(url).header("Range", "bytes=150000-150099").build()).execute().use {
                assertArrayEquals(expected.copyOfRange(150_000, 150_100), it.body!!.bytes())
            }
        } finally {
            CastServer.stop()
        }
    }

    private fun box(type: String, body: ByteArray = ByteArray(0)): ByteArray =
        ByteBuffer.allocate(8 + body.size).putInt(8 + body.size).put(type.toByteArray(Charsets.ISO_8859_1)).put(body).array()

    private fun moovSize(b: ByteArray) = ByteBuffer.wrap(b, MOOV_START, 4).int

    /** A stream laid out as YouTube's audio is: ftyp, moov (with mvex), sidx, then moof + mdat. */
    private fun stream(fragmented: Boolean = true, absolute: Boolean = false, tagged: Boolean = false, mdatBytes: Int = 1_000): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(box("ftyp", "dash".toByteArray() + ByteArray(4) + "iso6mp41".toByteArray()))
        check(out.size() == MOOV_START)
        val moovBody = ByteArrayOutputStream()
        moovBody.write(box("mvhd", ByteArray(100)))
        moovBody.write(box("trak", box("tkhd", ByteArray(84))))
        if (fragmented) moovBody.write(box("mvex", box("trex", ByteArray(24))))
        if (tagged) moovBody.write(box("udta", ByteArray(4)))
        out.write(box("moov", moovBody.toByteArray()))
        out.write(box("sidx", ByteArray(40)))
        // tfhd: version 0, flags 0x020000 (default-base-is-moof), or 0x000001 (base data offset present).
        val flags = if (absolute) byteArrayOf(0, 0, 0, 1) + ByteArray(4) + ByteArray(8) else byteArrayOf(0, 2, 0, 0) + ByteArray(4)
        out.write(box("moof", box("mfhd", ByteArray(8)) + box("traf", box("tfhd", flags) + box("trun", ByteArray(12)))))
        out.write(box("mdat", ByteArray(mdatBytes) { (it % 251).toByte() }))
        return out.toByteArray()
    }

    private companion object {
        const val MOOV_START = 24
    }
}
