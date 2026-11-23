package com.opentune.playback

import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import androidx.media3.common.C
import androidx.media3.common.DeviceInfo
import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import androidx.media3.common.Player
import androidx.media3.common.SimpleBasePlayer
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.opentune.cast.CastMedia
import com.opentune.cast.MaItem
import com.opentune.cast.MusicAssistant
import com.opentune.data.model.Song
import kotlin.math.abs
import kotlin.math.roundToInt
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.guava.future
import kotlinx.coroutines.launch

/**
 * The media session's player while casting to a Music Assistant speaker: a view of the
 * speaker's Music Assistant queue, so the app, the notification, the lock screen and the
 * volume keys show and drive the speaker. Music Assistant owns the queue; every command
 * goes to it, and what it says comes back as this player's state.
 *
 * Commands show at once and are held until Music Assistant says the same (or a while
 * passes): an Echo Show takes seconds to start, and the seek bar or play button jumping
 * back meanwhile looked like the press hadn't taken.
 *
 * Until it's [handOff]ed or [stage]d, it shows the speaker's own queue (joining it).
 */
@androidx.annotation.OptIn(androidx.media3.common.util.UnstableApi::class)
class MusicAssistantPlayer(
    private val ma: MusicAssistant,
    private val scope: CoroutineScope,
    /** A song as Music Assistant should get it; [lookUpNow] false where finding its stream can wait until it's fetched. */
    private val prepare: suspend (item: MediaItem, lookUpNow: Boolean) -> CastMedia?,
) : SimpleBasePlayer(Looper.getMainLooper()) {
    /** Songs not on the speaker yet: the phone's queue, shown until it's handed over. */
    private data class Local(
        val items: List<MediaItem>,
        val index: Int,
        val positionMs: Long,
        val sending: Boolean,
        /** The speaker's song before they were sent, and when they were. */
        val before: String? = null,
        val since: Long = SystemClock.elapsedRealtime(),
    )

    private data class PendingSeek(val index: Int, val positionMs: Long, val until: Long)

    private var local: Local? = null
    private var pendingSeek: PendingSeek? = null
    private var pendingPlay: Boolean? = null
    private var pendingPlayUntil = 0L

    /** The songs handed over, as sent and as the phone had them, to show them as themselves. */
    private val handed = ArrayList<Pair<CastMedia, MediaItem>>()
    private val itemsById = HashMap<String, MediaItem>()
    private val handler = Handler(Looper.getMainLooper())
    private val watching: Job = scope.launch {
        combine(ma.queue, ma.items, ma.status) { _, _, _ -> }.collect { invalidateState() }
    }

    /**
     * Hands the phone's [items] over, from the one at [index] at [positionMs]: they become the
     * speaker's queue. Shown at once; the speaker's own queue takes over once it holds them.
     */
    fun handOff(items: List<MediaItem>, index: Int, positionMs: Long) {
        startHandOff(items, index, positionMs)
        invalidateState()
    }

    /** Shows the phone's [items], paused, and hands them over when played. */
    fun stage(items: List<MediaItem>, index: Int, positionMs: Long) {
        if (items.isEmpty()) return
        local = Local(items, index.coerceIn(0, items.lastIndex), positionMs, sending = false)
        invalidateState()
    }

    // Kept apart from the public calls: a command's handler mustn't refresh the state itself.
    private fun startHandOff(items: List<MediaItem>, index: Int, positionMs: Long) {
        if (items.isEmpty()) return
        val at = index.coerceIn(0, items.lastIndex)
        local = Local(items, at, positionMs, sending = true, before = ma.queue.value.itemId)
        send(items.drop(at).take(HAND_OFF_MAX), positionMs)
        // A speaker that never takes them up says nothing more: the state is gone over again then.
        handler.postDelayed({ invalidateState() }, HAND_OFF_GIVE_UP_MS + 100)
    }

    private fun send(items: List<MediaItem>, positionMs: Long) {
        if (items.isEmpty()) return
        pendingPlay = true
        pendingPlayUntil = SystemClock.elapsedRealtime() + CONFIRM_MS
        refreshLater()
        scope.launch {
            // The first song as it plays, looked up now; the rest only when the speaker fetches them.
            val songs = items.mapIndexedNotNull { i, item -> prepare(item, i == 0)?.let { it to item } }
            synchronized(handed) {
                handed.clear()
                handed += songs
                itemsById.clear()
            }
            ma.handOff(songs.map { it.first }, positionMs)
        }
    }

    override fun getState(): State {
        val now = SystemClock.elapsedRealtime()
        val q = ma.queue.value
        val items = ma.items.value
        val builder = State.Builder()
            .setAvailableCommands(COMMANDS)
            .setDeviceInfo(DEVICE)
            .setDeviceVolume(((ma.status.value.volume ?: 0f) * 100).roundToInt())
            .setShuffleModeEnabled(q.shuffle)
            .setRepeatMode(
                when (q.repeat) {
                    "one" -> Player.REPEAT_MODE_ONE
                    "all" -> Player.REPEAT_MODE_ALL
                    else -> Player.REPEAT_MODE_OFF
                },
            )
        // The phone's songs until the speaker holds them: the first of them is playing there.
        local?.let { l ->
            val first = l.items.getOrNull(l.index)
            val playing = items.firstOrNull { it.id == q.itemId }
            val arrived = l.sending && q.itemId != null && q.itemId != l.before && playing != null && mediaItemOf(playing).mediaId == first?.mediaId
            // Not taken up after a while (MA turned it down): what the speaker has is shown.
            val gaveUp = l.sending && now - l.since > HAND_OFF_GIVE_UP_MS
            if (first == null || arrived || gaveUp) {
                local = null
            } else {
                return builder
                    .setPlaylist(l.items.mapIndexed { i, m -> MediaItemData.Builder("local-$i").setMediaItem(m).build() })
                    .setCurrentMediaItemIndex(l.index)
                    .setContentPositionMs(l.positionMs)
                    .setPlayWhenReady(l.sending, Player.PLAY_WHEN_READY_CHANGE_REASON_USER_REQUEST)
                    .setPlaybackState(if (l.sending) Player.STATE_BUFFERING else Player.STATE_READY)
                    .build()
            }
        }
        if (items.isEmpty()) {
            return builder.setPlaybackState(Player.STATE_IDLE).setPlayWhenReady(false, Player.PLAY_WHEN_READY_CHANGE_REASON_USER_REQUEST).build()
        }
        val index = items.indexOfFirst { it.id == q.itemId }.takeIf { it >= 0 } ?: q.index.coerceIn(0, items.lastIndex)
        val positionNow = q.positionNow(now)
        // A seek or play shown until the speaker says the same, or a while passes.
        pendingSeek?.let { s -> if (now > s.until || s.index == index && abs(positionNow - s.positionMs) < SEEK_TOLERANCE_MS) pendingSeek = null }
        pendingPlay?.let { p -> if (now > pendingPlayUntil || p == q.playing) pendingPlay = null }
        val seek = pendingSeek
        val playWhenReady = pendingPlay ?: q.playing
        val shownIndex = seek?.index?.coerceIn(0, items.lastIndex) ?: index
        val duration = items[shownIndex].durationMs
        val position = (seek?.positionMs ?: positionNow).let { if (duration > 0) it.coerceIn(0, duration) else it.coerceAtLeast(0) }
        return builder
            .setPlaylist(items.map { item ->
                MediaItemData.Builder(item.id)
                    .setMediaItem(mediaItemOf(item))
                    .setDurationUs(if (item.durationMs > 0) item.durationMs * 1000 else C.TIME_UNSET)
                    .setIsSeekable(true)
                    .build()
            })
            .setCurrentMediaItemIndex(shownIndex)
            .setContentPositionMs(
                if (seek == null && q.playing) PositionSupplier.getExtrapolating(position, 1f) else PositionSupplier.getConstant(position),
            )
            .setPlayWhenReady(playWhenReady, Player.PLAY_WHEN_READY_CHANGE_REASON_USER_REQUEST)
            // Asked to play and not playing yet: the speaker is starting.
            .setPlaybackState(if (playWhenReady && !q.playing) Player.STATE_BUFFERING else Player.STATE_READY)
            .build()
    }

    /** [item] as the phone shows it: as handed over, or a YouTube song, or as MA describes it. */
    private fun mediaItemOf(item: MaItem): MediaItem = synchronized(handed) {
        itemsById.getOrPut(item.id) {
            val uri = item.uri
            uri?.let {
                handed.firstOrNull { (sent, _) -> MusicAssistant.sameMedia(uri, sent.url) || sent.fallbackUrl?.let { f -> MusicAssistant.sameMedia(uri, f) } == true }?.second
            } ?: item.youTubeId?.let { id ->
                Song(
                    videoId = id,
                    title = item.title,
                    artist = item.artist,
                    thumbnailUrl = item.imageUrl,
                    durationText = item.durationMs.takeIf { it > 0 }?.let { ms -> "%d:%02d".format(java.util.Locale.ROOT, ms / 60_000, ms / 1000 % 60) },
                    albumName = item.album,
                ).toMediaItem()
            } ?: MediaItem.Builder()
                .setMediaId(uri ?: item.id)
                .setMediaMetadata(
                    MediaMetadata.Builder()
                        .setTitle(item.title)
                        .setArtist(item.artist)
                        .setAlbumTitle(item.album)
                        .setArtworkUri(item.imageUrl?.let(Uri::parse))
                        .build(),
                )
                .build()
        }
    }

    private fun currentIndex(): Int {
        val items = ma.items.value
        return items.indexOfFirst { it.id == ma.queue.value.itemId }.takeIf { it >= 0 } ?: ma.queue.value.index
    }

    private fun run(block: suspend () -> Unit): ListenableFuture<*> = scope.future { block() }

    /** Shows what was asked a little longer than the command takes, so the speaker can catch up. */
    private fun refreshLater() {
        handler.postDelayed({ invalidateState() }, CONFIRM_MS + 100)
    }

    override fun handleSetPlayWhenReady(playWhenReady: Boolean): ListenableFuture<*> {
        local?.let { l ->
            if (playWhenReady && !l.sending) startHandOff(l.items, l.index, l.positionMs)
            return Futures.immediateVoidFuture()
        }
        pendingPlay = playWhenReady
        pendingPlayUntil = SystemClock.elapsedRealtime() + CONFIRM_MS
        refreshLater()
        return run { if (playWhenReady) ma.play() else ma.pause() }
    }

    override fun handleSeek(mediaItemIndex: Int, positionMs: Long, seekCommand: Int): ListenableFuture<*> {
        val position = if (positionMs == C.TIME_UNSET) 0 else positionMs.coerceAtLeast(0)
        local?.let { l ->
            if (!l.sending) local = l.copy(index = mediaItemIndex.coerceIn(0, l.items.lastIndex), positionMs = position)
            return Futures.immediateVoidFuture()
        }
        // The song shown: one just picked, before the speaker has said it moved, counts.
        val current = pendingSeek?.index ?: currentIndex()
        pendingSeek = PendingSeek(mediaItemIndex, position, SystemClock.elapsedRealtime() + CONFIRM_MS)
        refreshLater()
        return run {
            when {
                mediaItemIndex == current -> ma.seek(position)
                seekCommand == Player.COMMAND_SEEK_TO_NEXT || seekCommand == Player.COMMAND_SEEK_TO_NEXT_MEDIA_ITEM -> ma.next()
                seekCommand == Player.COMMAND_SEEK_TO_PREVIOUS || seekCommand == Player.COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM -> ma.previous()
                else -> ma.playIndex(mediaItemIndex, position)
            }
        }
    }

    override fun handleSetMediaItems(mediaItems: List<MediaItem>, startIndex: Int, startPositionMs: Long): ListenableFuture<*> {
        val index = if (startIndex == C.INDEX_UNSET) 0 else startIndex.coerceIn(0, (mediaItems.size - 1).coerceAtLeast(0))
        val position = if (startPositionMs == C.TIME_UNSET) 0 else startPositionMs
        // Something new picked while casting: it goes to the speaker, unless nothing's been sent yet.
        if (local?.sending == false) {
            if (mediaItems.isNotEmpty()) local = Local(mediaItems, index, position, sending = false)
        } else {
            startHandOff(mediaItems, index, position)
        }
        return Futures.immediateVoidFuture()
    }

    override fun handleAddMediaItems(index: Int, mediaItems: List<MediaItem>): ListenableFuture<*> {
        local?.let { l ->
            if (!l.sending) local = l.copy(items = l.items.toMutableList().apply { addAll(index.coerceIn(0, size), mediaItems) })
            return Futures.immediateVoidFuture()
        }
        val next = index <= currentIndex() + 1
        return run { ma.add(mediaItems.mapNotNull { prepare(it, false)?.also { m -> synchronized(handed) { handed += m to it } } }, next) }
    }

    override fun handleRemoveMediaItems(fromIndex: Int, toIndex: Int): ListenableFuture<*> {
        local?.let { l ->
            if (!l.sending) {
                val left = l.items.filterIndexed { i, _ -> i !in fromIndex until toIndex }
                local = if (left.isEmpty()) null else l.copy(items = left, index = l.index.coerceAtMost(left.lastIndex))
            }
            return Futures.immediateVoidFuture()
        }
        val ids = ma.items.value.subList(fromIndex.coerceAtLeast(0), toIndex.coerceAtMost(ma.items.value.size)).map { it.id }
        return run { ids.forEach { ma.remove(it) } }
    }

    override fun handleMoveMediaItems(fromIndex: Int, toIndex: Int, newIndex: Int): ListenableFuture<*> {
        local?.let { l ->
            if (!l.sending) {
                val items = l.items.toMutableList()
                val moved = items.subList(fromIndex, toIndex).toList()
                repeat(moved.size) { items.removeAt(fromIndex) }
                items.addAll(newIndex.coerceIn(0, items.size), moved)
                local = l.copy(items = items)
            }
            return Futures.immediateVoidFuture()
        }
        val ids = ma.items.value.subList(fromIndex, toIndex.coerceAtMost(ma.items.value.size)).map { it.id }
        val shift = newIndex - fromIndex
        // Moved one at a time, keeping their order: the last first when moving later, the first first when earlier.
        return run { (if (shift > 0) ids.reversed() else ids).forEach { ma.move(it, shift) } }
    }

    override fun handleSetShuffleModeEnabled(shuffleModeEnabled: Boolean): ListenableFuture<*> = run { ma.shuffle(shuffleModeEnabled) }

    override fun handleSetRepeatMode(repeatMode: Int): ListenableFuture<*> = run {
        ma.repeat(
            when (repeatMode) {
                Player.REPEAT_MODE_ONE -> "one"
                Player.REPEAT_MODE_ALL -> "all"
                else -> "off"
            },
        )
    }

    override fun handleSetDeviceVolume(deviceVolume: Int, flags: Int): ListenableFuture<*> = run { ma.setVolume(deviceVolume / 100f) }

    override fun handleIncreaseDeviceVolume(flags: Int): ListenableFuture<*> = run {
        ma.setVolume(((ma.status.value.volume ?: 0f) + VOLUME_STEP).coerceAtMost(1f))
    }

    override fun handleDecreaseDeviceVolume(flags: Int): ListenableFuture<*> = run {
        ma.setVolume(((ma.status.value.volume ?: 0f) - VOLUME_STEP).coerceAtLeast(0f))
    }

    override fun handlePrepare(): ListenableFuture<*> = Futures.immediateVoidFuture()

    override fun handleStop(): ListenableFuture<*> = handleSetPlayWhenReady(false)

    override fun handleRelease(): ListenableFuture<*> {
        watching.cancel()
        handler.removeCallbacksAndMessages(null)
        return Futures.immediateVoidFuture()
    }

    private companion object {
        /** How many of the phone's songs, from the current one, go to the speaker. */
        const val HAND_OFF_MAX = 50
        /** How long the phone's songs are shown while the speaker hasn't taken them up. */
        const val HAND_OFF_GIVE_UP_MS = 30_000L
        /** How long a command is shown before the speaker's own word is taken over it: an Echo Show takes seconds to start. */
        const val CONFIRM_MS = 10_000L
        /** How near the speaker's place must be to a seek's to take the seek as done. */
        const val SEEK_TOLERANCE_MS = 2_000L
        const val VOLUME_STEP = 0.05f

        val DEVICE: DeviceInfo = DeviceInfo.Builder(DeviceInfo.PLAYBACK_TYPE_REMOTE).setMinVolume(0).setMaxVolume(100).build()

        val COMMANDS: Player.Commands = Player.Commands.Builder()
            .addAll(
                Player.COMMAND_PLAY_PAUSE,
                Player.COMMAND_PREPARE,
                Player.COMMAND_STOP,
                Player.COMMAND_SEEK_TO_DEFAULT_POSITION,
                Player.COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM,
                Player.COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM,
                Player.COMMAND_SEEK_TO_PREVIOUS,
                Player.COMMAND_SEEK_TO_NEXT_MEDIA_ITEM,
                Player.COMMAND_SEEK_TO_NEXT,
                Player.COMMAND_SEEK_TO_MEDIA_ITEM,
                Player.COMMAND_SET_SHUFFLE_MODE,
                Player.COMMAND_SET_REPEAT_MODE,
                Player.COMMAND_GET_CURRENT_MEDIA_ITEM,
                Player.COMMAND_GET_TIMELINE,
                Player.COMMAND_GET_METADATA,
                Player.COMMAND_SET_MEDIA_ITEM,
                Player.COMMAND_CHANGE_MEDIA_ITEMS,
                Player.COMMAND_GET_DEVICE_VOLUME,
                Player.COMMAND_SET_DEVICE_VOLUME_WITH_FLAGS,
                Player.COMMAND_ADJUST_DEVICE_VOLUME_WITH_FLAGS,
                Player.COMMAND_RELEASE,
            )
            .build()
    }
}
