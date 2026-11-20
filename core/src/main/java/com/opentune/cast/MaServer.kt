package com.opentune.cast

import android.content.Context
import android.content.SharedPreferences
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import com.opentune.data.DebugLog as Log
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.withContext
import org.json.JSONArray

/**
 * The Music Assistant server to cast through: the address set in Settings (or the one
 * found on the Wi-Fi) and its access token. The token is kept encrypted with a key from
 * the phone's keystore, is sent nowhere but that server, and never shows in the log.
 */
object MaServer {
    private const val TAG = "Cast"
    private const val KEY_ALIAS = "opentune-music-assistant"

    /** A server found on the Wi-Fi, from its `_mass._tcp` announcement. */
    data class Found(val address: String, val serverId: String?, val version: String?)

    /** What "Test connection" learned. */
    data class Report(val version: String, val players: Int, val youTubeMusic: Boolean)

    private var prefs: SharedPreferences? = null
    @Volatile private var token: String? = null

    private val _address = MutableStateFlow<String?>(null)
    /** The address set in Settings, if one is. */
    val address: StateFlow<String?> = _address.asStateFlow()

    private val _found = MutableStateFlow<Found?>(null)
    val found: StateFlow<Found?> = _found.asStateFlow()

    private val _hasToken = MutableStateFlow(false)
    val hasToken: StateFlow<Boolean> = _hasToken.asStateFlow()

    fun init(context: Context) {
        val p = context.getSharedPreferences("music_assistant", Context.MODE_PRIVATE)
        prefs = p
        _address.value = p.getString("address", null)
        val sealed = p.getString("token", null) ?: return
        token = runCatching { open(sealed) }.onFailure {
            Log.w(TAG, "couldn't read the saved Music Assistant token")
            p.edit().remove("token").apply()
        }.getOrNull()
        _hasToken.value = token != null
    }

    /** The address to use: the one set, else the one found. */
    fun current(): String? = _address.value ?: _found.value?.address

    fun token(): String? = token

    internal fun announce(server: Found) {
        _found.value = server
    }

    /** Keeps [address] (blank: use the one found) and [newToken] (null: keep the saved one). */
    fun save(address: String, newToken: String?) {
        val a = address.trim().ifBlank { null }
        _address.value = a
        prefs?.edit()?.putString("address", a)?.apply()
        if (newToken != null) {
            val t = newToken.trim()
            token = t.ifBlank { null }
            prefs?.edit()?.apply { if (t.isBlank()) remove("token") else putString("token", seal(t)) }?.apply()
            _hasToken.value = token != null
        }
        MaClient.forgetAll()
    }

    fun forget() {
        token = null
        _hasToken.value = false
        _address.value = null
        prefs?.edit()?.remove("token")?.remove("address")?.apply()
        runCatching { KeyStore.getInstance("AndroidKeyStore").apply { load(null) }.deleteEntry(KEY_ALIAS) }
        MaClient.forgetAll()
    }

    /** Connects to [address] with [testToken] (null: the saved one) and reports what's there. */
    suspend fun test(address: String, testToken: String?): Result<Report> = withContext(Dispatchers.IO) {
        val key = testToken?.trim()?.ifBlank { null } ?: token
        val client = MaClient(MaClient.wsUrl(address), { key })
        client.retain()
        try {
            runCatching {
                val info = client.connect()
                val players = (client.call("players/all") as? JSONArray)?.let { MaPlayers.castable(it).size } ?: 0
                Report(info.version, players, client.hasYouTubeMusic())
            }
        } finally {
            client.release()
        }
    }

    // ---- Keeping the token ---------------------------------------------------------------

    private fun key(): SecretKey {
        val store = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        (store.getKey(KEY_ALIAS, null) as? SecretKey)?.let { return it }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").apply {
            init(KeyGenParameterSpec.Builder(KEY_ALIAS, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .build())
        }.generateKey()
    }

    private fun seal(text: String): String {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply { init(Cipher.ENCRYPT_MODE, key()) }
        return Base64.encodeToString(cipher.iv + cipher.doFinal(text.toByteArray(Charsets.UTF_8)), Base64.NO_WRAP)
    }

    private fun open(sealed: String): String {
        val bytes = Base64.decode(sealed, Base64.NO_WRAP)
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply { init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(128, bytes, 0, 12)) }
        return String(cipher.doFinal(bytes, 12, bytes.size - 12), Charsets.UTF_8)
    }
}

/** A Music Assistant player, as the cast sheet lists it. */
data class MaPlayerRef(val server: String, val serverId: String?, val playerId: String, val queueId: String, val provider: String?)

internal object MaPlayers {
    /** The players worth casting to in a `players/all` reply: on, shown, and able to take a queue. */
    fun castable(all: JSONArray): List<org.json.JSONObject> = (0 until all.length()).mapNotNull { all.optJSONObject(it) }.filter { p ->
        p.optBoolean("available", true) && p.optBoolean("enabled", true) && !p.optBoolean("hide_in_ui", false) &&
            // A player inside a sync group is played through the group; protocol players sit under a real one.
            p.optString("synced_to").let { it.isBlank() || it == "null" } && p.optString("type") != "protocol"
    }

    fun target(p: org.json.JSONObject, server: String, serverId: String?): CastTarget {
        val id = p.getString("player_id")
        val name = p.optString("display_name").ifBlank { p.optString("name") }.ifBlank { id }
        val model = p.optJSONObject("device_info")?.optString("model")?.takeIf { it.isNotBlank() && it != "null" }
        val group = p.optString("type") == "group" || (p.optJSONArray("group_members")?.length() ?: 0) > 1
        // Every player has a queue of its own, by the same id; that's the one OpenTune fills.
        val queue = id
        return CastTarget(
            id = "ma:${serverId ?: server}:$id",
            name = name,
            kind = CastTarget.Kind.MUSIC_ASSISTANT,
            host = runCatching { java.net.URI(server).host }.getOrNull().orEmpty(),
            model = if (group) com.opentune.i18n.Tr.f("Group of {0}", p.optJSONArray("group_members")?.length() ?: 0) else model,
            ma = MaPlayerRef(server, serverId, id, queue, p.optString("provider").ifBlank { null }),
        )
    }

    /** A name as compared between lists: the same speaker found twice is written alike. */
    fun sameName(a: String, b: String): Boolean {
        fun norm(s: String) = s.lowercase().replace(Regex("[^a-z0-9]"), "")
        return norm(a).isNotEmpty() && norm(a) == norm(b)
    }
}
