package com.opentune.cast

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * Title, artist and album written into an MP4 stream's header, the way iTunes tags a file
 * (`moov/udta/meta/ilst`). Music Assistant names a song played from an address by the tags
 * ffprobe finds in it, and YouTube's audio carries none, so a song cast from the phone
 * showed as its address. With these, it shows as itself.
 *
 * Only a fragmented MP4 (YouTube's audio is one) is changed: its audio sits in fragments
 * placed relative to themselves, so the header can grow without moving anything that
 * points into the file. Anything else is left as it is.
 */
object Mp4Tags {
    data class Tags(val title: String, val artist: String?, val album: String?)

    /** A changed header: [header] stands in for the stream's first [replaced] bytes. */
    class Tagged(val header: ByteArray, val replaced: Int)

    /**
     * [head], the start of a stream, with [tags] as its `moov`'s `udta`; null when it isn't a
     * fragmented MP4 that can safely be changed, or [head] doesn't reach past its `moov`.
     */
    fun tag(head: ByteArray, tags: Tags): Tagged? {
        val top = boxes(head, 0, head.size) ?: return null
        val moov = top.firstOrNull { it.type == "moov" } ?: return null
        if (moov.end > head.size) return null
        val inside = boxes(head, moov.bodyStart, moov.end) ?: return null
        // Not fragmented: offsets into the file would move.
        if (inside.none { it.type == "mvex" }) return null
        // The moov's boxes must account for all of it, to be laid out again.
        if (inside.lastOrNull()?.end != moov.end) return null
        // A fragment that places its data by absolute offset would be moved too.
        val moof = top.firstOrNull { it.type == "moof" && it.start > moov.start } ?: return null
        if (moof.end > head.size || absoluteOffsets(head, moof)) return null
        if (moov.headerSize != 8) return null
        // YouTube's own udta, when there is one (some of its files carry one, untitled), gives way to these tags.
        val body = ByteArrayOutputStream(moov.end - moov.bodyStart)
        inside.filter { it.type != "udta" }.forEach { body.write(head, it.start, it.end - it.start) }
        body.write(udta(tags))
        val out = ByteArrayOutputStream(moov.start + 8 + body.size())
        out.write(head, 0, moov.start)
        out.write(ByteBuffer.allocate(4).putInt(8 + body.size()).array())
        out.write("moov".toByteArray(Charsets.ISO_8859_1))
        body.writeTo(out)
        return Tagged(out.toByteArray(), moov.end)
    }

    /** `udta` holding `meta` (handler `mdir`) holding `ilst` with ©nam, ©ART and ©alb. */
    fun udta(tags: Tags): ByteArray {
        val items = ByteArrayOutputStream()
        fun item(code: String, text: String?) {
            if (text.isNullOrBlank()) return
            val value = text.toByteArray(Charsets.UTF_8)
            // `data`: type 1 is UTF-8, then a zero locale.
            items.write(box(code, box("data", bytes(0, 0, 0, 1, 0, 0, 0, 0) + value)))
        }
        item("©nam", tags.title)
        item("©ART", tags.artist)
        item("©alb", tags.album)
        val hdlr = box("hdlr", ByteArray(4) + ByteArray(4) + "mdir".latin1() + "appl".latin1() + ByteArray(8) + ByteArray(1))
        val meta = box("meta", ByteArray(4) + hdlr + box("ilst", items.toByteArray()))
        return box("udta", meta)
    }

    private class Box(val type: String, val start: Int, val headerSize: Int, val end: Int) {
        val bodyStart get() = start + headerSize
    }

    /** The stream's top-level boxes and what's in its moov and first moof, for the log when it can't be tagged. */
    fun describe(head: ByteArray): String {
        val top = boxes(head, 0, head.size) ?: return "unreadable (${head.size} bytes)"
        fun inner(box: Box?) = box?.let { b -> boxes(head, b.bodyStart, minOf(b.end, head.size))?.joinToString(",") { it.type } }
        val moov = top.firstOrNull { it.type == "moov" }
        val moof = top.firstOrNull { it.type == "moof" }
        val traf = moof?.let { boxes(head, it.bodyStart, minOf(it.end, head.size))?.firstOrNull { b -> b.type == "traf" } }
        val tfhd = traf?.let { boxes(head, it.bodyStart, minOf(it.end, head.size))?.firstOrNull { b -> b.type == "tfhd" } }
        val flags = tfhd?.takeIf { it.bodyStart + 4 <= head.size }?.let { "%06x".format(u32(head, it.bodyStart) and 0xFFFFFF) }
        return "top=" + top.joinToString(",") { "${it.type}@${it.start}+${it.end - it.start}" } +
            " moov=[${inner(moov)}] moof=[${inner(moof)}] tfhd=$flags of ${head.size} bytes"
    }

    /** The boxes laid end to end from [from]; the last may run past [to]. Null if they don't parse. */
    private fun boxes(b: ByteArray, from: Int, to: Int): List<Box>? {
        val found = mutableListOf<Box>()
        var at = from
        while (at + 8 <= to) {
            var size = u32(b, at)
            var header = 8
            if (size == 1L) {
                if (at + 16 > to) break
                size = ByteBuffer.wrap(b, at + 8, 8).long
                header = 16
            } else if (size == 0L) {
                size = (Int.MAX_VALUE - at).toLong()
            }
            if (size < header) return null
            val type = String(b, at + 4, 4, Charsets.ISO_8859_1)
            val end = (at + size).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
            found += Box(type, at, header, end)
            if (end >= to) break
            at = end
        }
        return found
    }

    /** Whether any `traf` in [moof] gives a base data offset (tfhd flag 0x1), measured from the file's start. */
    private fun absoluteOffsets(b: ByteArray, moof: Box): Boolean {
        val trafs = boxes(b, moof.bodyStart, moof.end)?.filter { it.type == "traf" } ?: return true
        return trafs.any { traf ->
            val tfhd = boxes(b, traf.bodyStart, traf.end)?.firstOrNull { it.type == "tfhd" } ?: return@any true
            val flags = u32(b, tfhd.bodyStart) and 0xFFFFFF
            flags and 0x1L != 0L
        }
    }

    private fun box(type: String, body: ByteArray): ByteArray =
        ByteBuffer.allocate(8 + body.size).putInt(8 + body.size).put(type.latin1()).put(body).array()

    private fun String.latin1() = toByteArray(Charsets.ISO_8859_1)
    private fun bytes(vararg v: Int) = ByteArray(v.size) { v[it].toByte() }
    private fun u32(b: ByteArray, at: Int): Long = ByteBuffer.wrap(b, at, 4).int.toLong() and 0xFFFFFFFFL
}
