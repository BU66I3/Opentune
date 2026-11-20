package com.opentune.ui.settings

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import com.opentune.cast.MaServer
import com.opentune.ui.components.FloatingDialog
import com.opentune.ui.components.SheetButton
import com.opentune.ui.theme.Text
import kotlinx.coroutines.launch

/**
 * Connects a Music Assistant server, so its players (and the speakers Home Assistant
 * knows) show in Play on. The address fills in from the server found on the Wi-Fi; the
 * access token is made in Music Assistant's settings and kept encrypted on this phone.
 */
@Composable
fun MusicAssistantDialog(onDismiss: () -> Unit) {
    val scope = rememberCoroutineScope()
    val found by MaServer.found.collectAsState()
    val saved by MaServer.hasToken.collectAsState()
    var address by remember { mutableStateOf(MaServer.address.value ?: found?.address.orEmpty()) }
    var token by remember { mutableStateOf("") }
    var busy by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf<String?>(null) }
    var error by remember { mutableStateOf<String?>(null) }

    FloatingDialog(
        onDismissRequest = onDismiss,
        title = { Text("Music Assistant") },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(
                    "Play on the speakers Music Assistant runs, and Home Assistant sees what's playing. " +
                        "Make a long-lived access token in Music Assistant's settings and paste it here.",
                    style = MaterialTheme.typography.bodyMedium,
                )
                OutlinedTextField(
                    address, { address = it; result = null },
                    label = { Text("Server") },
                    placeholder = { Text(found?.address ?: "homeassistant.local:8095") },
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri),
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    token, { token = it; result = null },
                    label = { Text(if (saved) "Access token (saved)" else "Access token") },
                    placeholder = { if (saved) Text("Leave empty to keep the saved one") },
                    singleLine = true,
                    visualTransformation = PasswordVisualTransformation(),
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
                    modifier = Modifier.fillMaxWidth(),
                )
                SheetButton("Test connection", loading = busy, enabled = !busy && (address.isNotBlank() || found != null) && (token.isNotBlank() || saved), onClick = {
                    busy = true
                    error = null
                    result = null
                    scope.launch {
                        MaServer.test(address.ifBlank { found?.address.orEmpty() }, token.ifBlank { null })
                            .onSuccess { r ->
                                result = com.opentune.i18n.Tr.f("Music Assistant {0} · {1} players", r.version, r.players) + " · " +
                                    com.opentune.i18n.Tr.t(if (r.youTubeMusic) "YouTube Music ready" else "No YouTube Music: songs play through this phone")
                            }
                            .onFailure { error = it.message ?: com.opentune.i18n.Tr.t("Couldn't connect") }
                        busy = false
                    }
                })
                result?.let { Text(it, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.primary) }
                error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }
                Text(
                    "The token is stored encrypted and sent only to this server.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        },
        confirmButton = {
            SheetButton(
                text = "Save",
                enabled = !busy && (token.isNotBlank() || saved),
                onClick = {
                    // Kept as it is, so Play on asks the server straight away instead of waiting on mDNS.
                    MaServer.save(address.ifBlank { found?.address.orEmpty() }, token.ifBlank { null })
                    onDismiss()
                },
            )
        },
        dismissButton = { SheetButton("Cancel", onClick = onDismiss, closes = true) },
    )
}
