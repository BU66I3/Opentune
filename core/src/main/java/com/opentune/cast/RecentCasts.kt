package com.opentune.cast

import android.content.Context
import android.content.SharedPreferences
import com.opentune.data.DebugLog as Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import org.json.JSONArray
import org.json.JSONObject

/**
 * The devices last cast to, newest first, kept on the phone so Play on can offer them at
 * once, before a search has found anything. Each is kept as it was when last connected; a
 * search that finds it again has the fresher address, and Play on uses that.
 */
object RecentCasts {
    private const val TAG = "Cast"
    const val MAX = 3

    private var prefs: SharedPreferences? = null
    private val _list = MutableStateFlow<List<CastTarget>>(emptyList())
    val list: StateFlow<List<CastTarget>> = _list.asStateFlow()

    fun init(context: Context) {
        val p = context.getSharedPreferences("cast", Context.MODE_PRIVATE)
        prefs = p
        _list.value = runCatching { fromJson(p.getString("recent", null) ?: return) }
            .onFailure { Log.w(TAG, "couldn't read recent casts: ${it.message}") }
            .getOrDefault(emptyList())
    }

    /** Puts [target] first, once, keeping [MAX]. */
    fun remember(target: CastTarget) {
        val next = (listOf(target) + _list.value.filterNot { it.id == target.id }).take(MAX)
        _list.value = next
        prefs?.edit()?.putString("recent", toJson(next))?.apply()
    }

    fun forget(id: String) {
        val next = _list.value.filterNot { it.id == id }
        _list.value = next
        prefs?.edit()?.putString("recent", toJson(next))?.apply()
    }

    internal fun toJson(targets: List<CastTarget>): String = JSONArray().apply {
        targets.forEach { t ->
            put(JSONObject().apply {
                put("id", t.id)
                put("name", t.name)
                put("kind", t.kind.name)
                put("host", t.host)
                put("port", t.port)
                t.model?.let { put("model", it) }
                t.dlna?.let { d ->
                    put("dlna", JSONObject().put("name", d.name).put("transportUrl", d.transportUrl).put("renderingUrl", d.renderingUrl).put("id", d.id))
                }
                t.ma?.let { m ->
                    put("ma", JSONObject().put("server", m.server).put("serverId", m.serverId).put("playerId", m.playerId)
                        .put("queueId", m.queueId).put("provider", m.provider))
                }
            })
        }
    }.toString()

    /** What [toJson] wrote; an entry that can't be read (a kind this version doesn't know) is left out. */
    internal fun fromJson(text: String): List<CastTarget> {
        val all = JSONArray(text)
        return (0 until all.length()).mapNotNull { i ->
            runCatching {
                val o = all.getJSONObject(i)
                fun JSONObject.text(key: String) = if (isNull(key)) null else optString(key).ifBlank { null }
                val kind = CastTarget.Kind.valueOf(o.getString("kind"))
                val dlna = o.optJSONObject("dlna")?.let { DlnaDevice(it.getString("name"), it.getString("transportUrl"), it.text("renderingUrl"), it.getString("id")) }
                val ma = o.optJSONObject("ma")?.let { MaPlayerRef(it.getString("server"), it.text("serverId"), it.getString("playerId"), it.getString("queueId"), it.text("provider")) }
                // A device that can't be reached without its details is no use.
                if (kind == CastTarget.Kind.DLNA && dlna == null || kind == CastTarget.Kind.MUSIC_ASSISTANT && ma == null) return@runCatching null
                CastTarget(o.getString("id"), o.getString("name"), kind, o.getString("host"), o.optInt("port"), o.text("model"), dlna, ma)
            }.getOrNull()
        }
    }
}
