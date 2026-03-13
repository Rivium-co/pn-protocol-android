package co.rivium.protocol

import android.os.Handler
import android.os.Looper
import android.util.Log
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import kotlin.math.pow

/**
 * PNSocket - Main connection handler for PN Protocol
 *
 * Wraps MQTT client with PN Protocol branded API.
 *
 * | PN Protocol   | MQTT              |
 * |---------------|-------------------|
 * | open()        | connect()         |
 * | close()       | disconnect()      |
 * | stream()      | subscribe()       |
 * | detach()      | unsubscribe()     |
 * | dispatch()    | publish()         |
 * | channel       | topic             |
 */
class PNSocket(
    private val config: PNConfig
) {
    companion object {
        private const val TAG = "PNSocket"
        private const val BACKOFF_MULTIPLIER = 2.0
        private const val JITTER_FACTOR = 0.2
    }

    // MQTT client (internal implementation)
    private var mqttClient: MqttAsyncClient? = null

    // State
    @Volatile
    private var state: PNState = PNState.DISCONNECTED
    private val activeChannels: MutableSet<String> = java.util.Collections.newSetFromMap(ConcurrentHashMap())

    // Listeners (thread-safe)
    private val connectionListeners = CopyOnWriteArrayList<PNConnectionListener>()
    private val messageListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<PNMessageListener>>()
    private val errorListeners = CopyOnWriteArrayList<PNErrorListener>()

    // Reconnection state
    private val retryAttempt = AtomicInteger(0)
    private val isRetrying = AtomicBoolean(false)
    private val isConnecting = AtomicBoolean(false)
    private val manualDisconnect = AtomicBoolean(false)
    private var retryRunnable: Runnable? = null
    private val mainHandler = Handler(Looper.getMainLooper())

    // ========================================================================
    // Public API
    // ========================================================================

    /**
     * Open connection to the gateway (MQTT: connect)
     */
    fun open(): PNSocket {
        if (state == PNState.CONNECTED || state == PNState.CONNECTING) {
            return this
        }

        manualDisconnect.set(false)
        resetRetryState()
        connectInternal()
        return this
    }

    /**
     * Close connection gracefully (MQTT: disconnect)
     */
    fun close(): PNSocket {
        if (state == PNState.DISCONNECTED) return this

        Log.d(TAG, "Closing connection")
        manualDisconnect.set(true)
        cancelRetry()
        isConnecting.set(false)

        state = PNState.DISCONNECTING
        notifyStateChange(PNState.DISCONNECTING)

        try {
            mqttClient?.disconnect()?.waitForCompletion(5000)
            mqttClient?.close()
        } catch (e: Exception) {
            Log.w(TAG, "Error during close: ${e.message}")
        } finally {
            mqttClient = null
            activeChannels.clear()
            state = PNState.DISCONNECTED
            notifyStateChange(PNState.DISCONNECTED)
        }

        return this
    }

    /**
     * Reconnect immediately without destroying the socket.
     * Cancels any pending retry and triggers an immediate connection attempt.
     * Unlike close()+open(), this preserves activeChannels so resubscription works.
     */
    fun reconnectImmediately(): PNSocket {
        if (state == PNState.CONNECTED || state == PNState.CONNECTING) {
            Log.d(TAG, "reconnectImmediately() skipped - state is $state")
            return this
        }

        Log.d(TAG, "Reconnecting immediately")
        cancelRetry()
        isConnecting.set(false)
        manualDisconnect.set(false)
        connectInternal()
        return this
    }

    /**
     * Stream messages from a channel (MQTT: subscribe)
     *
     * @param channel Channel name to listen to (MQTT: topic)
     * @param mode Delivery guarantee mode (MQTT: QoS)
     * @param listener Message listener
     */
    fun stream(
        channel: String,
        mode: PNDeliveryMode = PNDeliveryMode.RELIABLE,
        listener: PNMessageListener
    ): PNSocket {
        ensureConnected()

        // Add listener, deduplicating to prevent accumulation across reconnects
        val listeners = messageListeners.getOrPut(channel) { CopyOnWriteArrayList() }
        if (!listeners.contains(listener)) {
            listeners.add(listener)
        }

        if (!activeChannels.contains(channel)) {
            try {
                mqttClient?.subscribe(channel, mode.qos, null, object : IMqttActionListener {
                    override fun onSuccess(token: IMqttToken?) {
                        Log.d(TAG, "Streaming from channel: $channel")
                        activeChannels.add(channel)
                    }

                    override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                        val error = PNError(
                            PNError.Code.STREAM_FAILED,
                            "Failed to stream from $channel: ${exception?.message}",
                            exception
                        )
                        notifyError(error)
                    }
                })
            } catch (e: Exception) {
                val error = PNError.fromException(e, "Stream failed")
                notifyError(error)
            }
        }

        return this
    }

    /**
     * Stream with pattern matching (wildcard channels)
     *
     * Patterns:
     * - `+` matches one segment: `chat/+/messages` matches `chat/room1/messages`
     * - `#` matches all remaining: `sensors/#` matches `sensors/temp/room1`
     */
    fun streamPattern(
        pattern: String,
        mode: PNDeliveryMode = PNDeliveryMode.RELIABLE,
        listener: PNMessageListener
    ): PNSocket {
        return stream(pattern, mode, listener)
    }

    /**
     * Stop streaming from a channel (MQTT: unsubscribe)
     */
    fun detach(channel: String): PNSocket {
        if (!activeChannels.contains(channel)) return this

        try {
            mqttClient?.unsubscribe(channel, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) {
                    Log.d(TAG, "Detached from channel: $channel")
                    activeChannels.remove(channel)
                    messageListeners.remove(channel)
                }

                override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                    val error = PNError(
                        PNError.Code.DETACH_FAILED,
                        "Failed to detach from $channel: ${exception?.message}",
                        exception
                    )
                    notifyError(error)
                }
            })
        } catch (e: Exception) {
            Log.w(TAG, "Detach error: ${e.message}")
        }

        return this
    }

    /**
     * Dispatch a message to a channel (MQTT: publish)
     */
    fun dispatch(message: PNMessage): PNSocket {
        ensureConnected()

        try {
            val mqttMessage = MqttMessage(message.payload).apply {
                qos = message.mode.qos
                isRetained = message.persist
            }
            mqttClient?.publish(message.channel, mqttMessage)
            Log.d(TAG, "Dispatched message to ${message.channel}")
        } catch (e: Exception) {
            val error = PNError(
                PNError.Code.DISPATCH_FAILED,
                "Failed to dispatch: ${e.message}",
                e
            )
            notifyError(error)
        }

        return this
    }

    /**
     * Dispatch with callback
     */
    fun dispatch(message: PNMessage, callback: PNDispatchCallback): PNSocket {
        ensureConnected()

        try {
            val mqttMessage = MqttMessage(message.payload).apply {
                qos = message.mode.qos
                isRetained = message.persist
            }
            mqttClient?.publish(message.channel, mqttMessage, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) {
                    mainHandler.post { callback.onSuccess(message.id) }
                }

                override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                    val error = PNError(
                        PNError.Code.DISPATCH_FAILED,
                        exception?.message ?: "Dispatch failed",
                        exception
                    )
                    mainHandler.post { callback.onFailure(error) }
                }
            })
        } catch (e: Exception) {
            val error = PNError.fromException(e, "Dispatch failed")
            callback.onFailure(error)
        }

        return this
    }

    /**
     * Get current connection state
     */
    fun state(): PNState = state

    /**
     * Check if connected
     */
    fun isConnected(): Boolean = state == PNState.CONNECTED && mqttClient?.isConnected == true

    /**
     * Get active channels
     */
    fun activeChannels(): Set<String> = activeChannels.toSet()

    // ========================================================================
    // Listeners
    // ========================================================================

    fun addConnectionListener(listener: PNConnectionListener): PNSocket {
        connectionListeners.add(listener)
        return this
    }

    fun removeConnectionListener(listener: PNConnectionListener): PNSocket {
        connectionListeners.remove(listener)
        return this
    }

    fun addErrorListener(listener: PNErrorListener): PNSocket {
        errorListeners.add(listener)
        return this
    }

    fun removeErrorListener(listener: PNErrorListener): PNSocket {
        errorListeners.remove(listener)
        return this
    }

    // ========================================================================
    // Internal MQTT Implementation
    // ========================================================================

    private fun connectInternal() {
        // Prevent concurrent connection attempts that cause EMQX session takeover
        if (!isConnecting.compareAndSet(false, true)) {
            Log.d(TAG, "Connection already in progress - skipping")
            return
        }

        try {
            // Clean up existing client
            mqttClient?.let { client ->
                try {
                    if (client.isConnected) client.disconnect()
                    client.close()
                } catch (e: Exception) {
                    Log.w(TAG, "Error cleaning up old client: ${e.message}")
                }
            }
            mqttClient = null

            state = PNState.CONNECTING
            notifyStateChange(PNState.CONNECTING)

            // Build MQTT server URI
            val protocol = if (config.secure) "ssl" else "tcp"
            val serverUri = "$protocol://${config.gateway}:${config.port}"

            Log.d(TAG, "Connecting to gateway: $serverUri (clientId: ${config.clientId})")

            // Create MQTT client
            mqttClient = MqttAsyncClient(serverUri, config.clientId, MemoryPersistence())
            mqttClient?.setCallback(createMqttCallback())

            // Build connection options
            val options = MqttConnectOptions().apply {
                isCleanSession = config.freshStart
                isAutomaticReconnect = false  // We handle reconnection ourselves
                connectionTimeout = config.connectionTimeout
                keepAliveInterval = config.heartbeatInterval

                // Authentication
                config.auth?.let { auth ->
                    when (auth) {
                        is PNAuth.Basic -> {
                            userName = auth.username
                            password = auth.password.toCharArray()
                        }
                        is PNAuth.Token -> {
                            userName = "token"
                            password = auth.token.toCharArray()
                        }
                    }
                }

                // Exit signal (Last Will)
                config.exitSignal?.let { exit ->
                    setWill(exit.channel, exit.payload, exit.mode.qos, exit.persist)
                }
            }

            // Connect
            mqttClient?.connect(options, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) {
                    Log.d(TAG, "Connection initiated successfully")
                }

                override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Connection failed: ${exception?.message}", exception as? Exception)
                    exception?.cause?.let { cause ->
                        Log.e(TAG, "Connection failed cause: ${cause::class.java.name}: ${cause.message}")
                        cause.cause?.let { root ->
                            Log.e(TAG, "Connection failed root cause: ${root::class.java.name}: ${root.message}")
                        }
                    }
                    isConnecting.set(false)
                    state = PNState.DISCONNECTED

                    val error = PNError.connectionFailed(
                        exception?.message ?: "Connection failed",
                        exception
                    )
                    notifyError(error)

                    if (!manualDisconnect.get()) {
                        scheduleRetry()
                    }
                }
            })

        } catch (e: Exception) {
            Log.e(TAG, "Connection error: ${e.message}", e)
            e.cause?.let { cause ->
                Log.e(TAG, "Connection error cause: ${cause::class.java.name}: ${cause.message}")
            }
            isConnecting.set(false)
            state = PNState.DISCONNECTED

            val error = PNError.fromException(e, "Connection failed")
            notifyError(error)

            if (!manualDisconnect.get()) {
                scheduleRetry()
            }
        }
    }

    private fun createMqttCallback() = object : MqttCallbackExtended {
        override fun connectComplete(reconnect: Boolean, serverURI: String?) {
            Log.d(TAG, "Connected to $serverURI (reconnect: $reconnect)")

            isConnecting.set(false)
            state = PNState.CONNECTED
            resetRetryState()
            notifyStateChange(PNState.CONNECTED)
            notifyConnected()

            // Resubscribe to active channels (needed because cleanSession=true)
            resubscribeChannels()
        }

        override fun connectionLost(cause: Throwable?) {
            Log.e(TAG, "Connection lost: ${cause?.message}")

            isConnecting.set(false)
            state = PNState.DISCONNECTED
            // NOTE: activeChannels is intentionally NOT cleared here
            // so resubscribeChannels() can restore them after reconnect

            val error = PNError.connectionLost(
                cause?.message ?: "Connection lost",
                cause
            )
            notifyError(error)
            notifyDisconnected(cause?.message)

            if (!manualDisconnect.get() && config.autoReconnect) {
                scheduleRetry()
            }
        }

        override fun messageArrived(topic: String?, message: MqttMessage?) {
            if (topic == null || message == null) return

            val pnMessage = PNMessage.fromMqtt(
                topic = topic,
                payload = message.payload,
                qos = message.qos,
                retained = message.isRetained
            )

            Log.d(TAG, "Message arrived on channel: $topic")

            // Collect all listeners that should receive this message (avoid duplicates)
            val listenersToNotify = mutableSetOf<PNMessageListener>()

            // Direct channel match
            messageListeners[topic]?.let { listeners ->
                listenersToNotify.addAll(listeners)
            }

            // Pattern listeners (only for patterns with wildcards, skip exact matches already handled)
            messageListeners.keys
                .filter { pattern -> pattern != topic && (pattern.contains('+') || pattern.contains('#')) }
                .filter { pattern -> matchesPattern(pattern, topic) }
                .forEach { pattern ->
                    messageListeners[pattern]?.let { listeners ->
                        listenersToNotify.addAll(listeners)
                    }
                }

            // Notify all unique listeners
            listenersToNotify.forEach { listener ->
                mainHandler.post { listener.onMessage(pnMessage) }
            }
        }

        override fun deliveryComplete(token: IMqttDeliveryToken?) {}
    }

    private fun resubscribeChannels() {
        if (activeChannels.isEmpty()) return

        val channels = activeChannels.toTypedArray()
        val qos = IntArray(channels.size) { PNDeliveryMode.RELIABLE.qos }

        try {
            mqttClient?.subscribe(channels, qos, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) {
                    Log.d(TAG, "Resubscribed to ${channels.size} channels")
                }

                override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Resubscribe failed: ${exception?.message}")
                }
            })
        } catch (e: Exception) {
            Log.e(TAG, "Resubscribe error: ${e.message}")
        }
    }

    private fun matchesPattern(pattern: String, topic: String): Boolean {
        if (!pattern.contains('+') && !pattern.contains('#')) {
            return pattern == topic
        }
        // Simple pattern matching for MQTT wildcards
        val patternParts = pattern.split('/')
        val topicParts = topic.split('/')

        var i = 0
        for (part in patternParts) {
            when (part) {
                "#" -> return true  // Matches everything remaining
                "+" -> i++          // Matches one segment
                else -> {
                    if (i >= topicParts.size || topicParts[i] != part) return false
                    i++
                }
            }
        }
        return i == topicParts.size
    }

    // ========================================================================
    // Reconnection with Exponential Backoff
    // ========================================================================

    private fun calculateRetryDelay(attempt: Int): Long {
        val exponentialDelay = config.reconnectDelay * BACKOFF_MULTIPLIER.pow(attempt.toDouble())
        val cappedDelay = min(exponentialDelay.toLong(), config.maxReconnectDelay)
        val jitter = (cappedDelay * JITTER_FACTOR * (Math.random() * 2 - 1)).toLong()
        return cappedDelay + jitter
    }

    private fun scheduleRetry() {
        if (manualDisconnect.get()) return

        val currentAttempt = retryAttempt.get()
        if (config.maxReconnectAttempts > 0 && currentAttempt >= config.maxReconnectAttempts) {
            Log.e(TAG, "Max retry attempts (${config.maxReconnectAttempts}) reached")
            val error = PNError(
                PNError.Code.CONNECTION_FAILED,
                "Max retry attempts reached after $currentAttempt attempts"
            )
            notifyError(error)
            return
        }

        val delayMs = calculateRetryDelay(currentAttempt)
        Log.d(TAG, "Scheduling retry ${currentAttempt + 1} in ${delayMs}ms")

        state = PNState.RECONNECTING
        notifyStateChange(PNState.RECONNECTING)
        notifyReconnecting(currentAttempt, delayMs)

        isRetrying.set(true)
        retryRunnable = Runnable {
            val newAttempt = retryAttempt.incrementAndGet()
            Log.d(TAG, "Executing retry attempt $newAttempt")
            connectInternal()
        }
        mainHandler.postDelayed(retryRunnable!!, delayMs)
    }

    private fun cancelRetry() {
        retryRunnable?.let { mainHandler.removeCallbacks(it) }
        retryRunnable = null
        isRetrying.set(false)
    }

    private fun resetRetryState() {
        retryAttempt.set(0)
        isRetrying.set(false)
        cancelRetry()
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private fun ensureConnected() {
        if (state != PNState.CONNECTED) {
            throw PNException(PNError.notConnected())
        }
    }

    private fun notifyStateChange(newState: PNState) {
        mainHandler.post {
            connectionListeners.forEach { it.onStateChanged(newState) }
        }
    }

    private fun notifyConnected() {
        mainHandler.post {
            connectionListeners.forEach { it.onConnected() }
        }
    }

    private fun notifyDisconnected(reason: String?) {
        mainHandler.post {
            connectionListeners.forEach { it.onDisconnected(reason) }
        }
    }

    private fun notifyReconnecting(attempt: Int, nextRetryMs: Long) {
        mainHandler.post {
            connectionListeners.forEach { it.onReconnecting(attempt, nextRetryMs) }
        }
    }

    private fun notifyError(error: PNError) {
        mainHandler.post {
            errorListeners.forEach { it.onError(error) }
        }
    }
}
