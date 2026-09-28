package co.rivium.protocol

import android.os.Handler
import android.os.Looper
import android.util.Log
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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

        /** Pause before trying the next endpoint after a failed attempt. */
        private const val FAILOVER_DELAY_MS = 250L

        /** Default time to wait for a ping response in [probe]. */
        const val DEFAULT_PROBE_TIMEOUT_MS = 15_000L

        /** Tears down abandoned clients off the calling thread. */
        private val cleanupExecutor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "pn-socket-cleanup").apply { isDaemon = true }
        }
    }

    // MQTT client (internal implementation)
    @Volatile
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
    private val manualDisconnect = AtomicBoolean(false)
    @Volatile
    private var retryRunnable: Runnable? = null
    private val mainHandler = Handler(Looper.getMainLooper())

    // One connect attempt at a time; callbacks of abandoned clients are ignored
    private val connectGuard = PNConnectGuard()

    // Endpoint failover
    private val rotation = PNEndpointRotation()
    @Volatile
    private var endpointProvider: PNEndpointProvider? = null
    private val endpointListeners = CopyOnWriteArrayList<PNEndpointListener>()
    @Volatile
    private var connectedEndpoint: PNEndpoint? = null

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
        rotation.reset()
        connectInternal()
        return this
    }

    /**
     * Close connection gracefully (MQTT: disconnect)
     */
    fun close(): PNSocket {
        manualDisconnect.set(true)
        cancelRetry()
        rotation.reset()
        // Any attempt still in flight is abandoned: its callbacks become no-ops
        connectGuard.invalidate()

        if (state == PNState.DISCONNECTED) {
            mqttClient?.let { shutdownQuietly(it) }
            mqttClient = null
            connectedEndpoint = null
            return this
        }

        Log.d(TAG, "Closing connection")

        state = PNState.DISCONNECTING
        notifyStateChange(PNState.DISCONNECTING)

        val client = mqttClient
        try {
            client?.disconnect()?.waitForCompletion(5000)
            client?.close()
        } catch (e: Exception) {
            Log.w(TAG, "Error during close: ${e.message}")
            client?.let { shutdownQuietly(it) }
        } finally {
            mqttClient = null
            connectedEndpoint = null
            activeChannels.clear()
            state = PNState.DISCONNECTED
            notifyStateChange(PNState.DISCONNECTED)
        }

        return this
    }

    /**
     * Reconnect immediately without destroying the socket.
     * Cancels any pending retry, resets the backoff and triggers an immediate
     * connection attempt. No-op while connected or connecting.
     * Unlike close()+open(), this preserves activeChannels so resubscription works.
     */
    fun reconnectImmediately(): PNSocket = reconnectImmediately(force = false)

    /**
     * Like [reconnectImmediately]. With [force] = true an existing connection or
     * in-flight attempt is dropped first — use it when the network changed and the
     * current socket is bound to a network that is gone.
     */
    fun reconnectImmediately(force: Boolean): PNSocket {
        if (!force && (state == PNState.CONNECTED || state == PNState.CONNECTING)) {
            Log.d(TAG, "reconnectImmediately() skipped - state is $state")
            return this
        }

        Log.d(TAG, "Reconnecting immediately (force=$force, state=$state)")
        manualDisconnect.set(false)
        resetRetryState()
        rotation.reset()

        if (force) {
            val wasConnected = state == PNState.CONNECTED
            connectGuard.invalidate()
            mqttClient?.let { shutdownQuietly(it) }
            mqttClient = null
            connectedEndpoint = null
            state = PNState.DISCONNECTED
            if (wasConnected) {
                notifyStateChange(PNState.DISCONNECTED)
                notifyDisconnected("Reconnecting")
            }
        }

        connectInternal()
        return this
    }

    /**
     * Check that an established connection is still alive, e.g. after the device
     * woke up. Sends a ping if nothing was received recently; if the gateway does
     * not answer within [timeoutMs], the connection is replaced.
     * Does nothing unless connected.
     */
    @JvmOverloads
    fun probe(timeoutMs: Long = DEFAULT_PROBE_TIMEOUT_MS): PNSocket {
        if (state != PNState.CONNECTED) return this
        val client = mqttClient ?: return this
        val gen = connectGuard.current()

        if (!client.isConnected) {
            Log.w(TAG, "probe(): client no longer connected - reconnecting")
            reconnectImmediately(force = true)
            return this
        }

        val token = try {
            client.checkPing(null, null)
        } catch (e: Exception) {
            Log.w(TAG, "probe(): ping failed (${e.message}) - reconnecting")
            reconnectImmediately(force = true)
            return this
        }

        if (token == null) {
            Log.d(TAG, "probe(): recent traffic, connection alive")
            return this
        }

        Log.d(TAG, "probe(): ping sent, waiting up to ${timeoutMs}ms")
        mainHandler.postDelayed({
            if (!token.isComplete && state == PNState.CONNECTED &&
                mqttClient === client && connectGuard.isCurrent(gen)
            ) {
                Log.w(TAG, "probe(): no ping response within ${timeoutMs}ms - reconnecting")
                reconnectImmediately(force = true)
            }
        }, timeoutMs)
        return this
    }

    /**
     * Endpoints to try, in order, at the start of each connection round.
     * Without a provider (or when it returns an empty list) the gateway/port/secure
     * from [PNConfig] is used, as before.
     */
    fun setEndpointProvider(provider: PNEndpointProvider?): PNSocket {
        endpointProvider = provider
        return this
    }

    fun addEndpointListener(listener: PNEndpointListener): PNSocket {
        endpointListeners.add(listener)
        return this
    }

    fun removeEndpointListener(listener: PNEndpointListener): PNSocket {
        endpointListeners.remove(listener)
        return this
    }

    /** Endpoint of the current connection, or null when not connected. */
    fun connectedEndpoint(): PNEndpoint? = connectedEndpoint

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
                Log.d(TAG, "Subscribing to channel: $channel (QoS=${mode.qos})")
                mqttClient?.subscribe(channel, mode.qos, null, object : IMqttActionListener {
                    override fun onSuccess(token: IMqttToken?) {
                        Log.d(TAG, "Streaming from channel: $channel")
                        activeChannels.add(channel)
                    }

                    override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                        Log.e(TAG, "Failed to stream from channel: $channel - ${exception?.message}")
                        exception?.cause?.let {
                            Log.e(TAG, "Stream failure cause: ${it::class.java.name}: ${it.message}")
                        }
                        val error = PNError(
                            PNError.Code.STREAM_FAILED,
                            "Failed to stream from $channel: ${exception?.message}",
                            exception
                        )
                        notifyError(error)
                    }
                })
            } catch (e: Exception) {
                Log.e(TAG, "Stream exception for channel: $channel - ${e.message}")
                val error = PNError.fromException(e, "Stream failed")
                notifyError(error)
            }
        } else {
            Log.d(TAG, "Already streaming from channel: $channel")
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

    private fun defaultEndpoint() = PNEndpoint(config.gateway, config.port, config.secure)

    private fun resolveEndpoints(): List<PNEndpoint> {
        val provided = try {
            endpointProvider?.endpoints().orEmpty()
        } catch (e: Exception) {
            Log.w(TAG, "Endpoint provider failed: ${e.message}")
            emptyList()
        }
        return provided.ifEmpty { listOf(defaultEndpoint()) }
    }

    private fun connectInternal() {
        // Prevent concurrent connection attempts that cause EMQX session takeover
        val gen = connectGuard.tryBegin()
        if (gen == null) {
            Log.d(TAG, "Connection already in progress - skipping")
            return
        }

        var client: MqttAsyncClient? = null
        try {
            // Clean up existing client
            mqttClient?.let { shutdownQuietly(it) }
            mqttClient = null

            if (!rotation.inRound()) {
                rotation.startRound(resolveEndpoints())
            }
            val endpoint = rotation.current() ?: defaultEndpoint()

            state = PNState.CONNECTING
            notifyStateChange(PNState.CONNECTING)

            // Build MQTT server URI
            val serverUri = endpoint.uri

            Log.d(TAG, "Connecting to gateway: $serverUri (clientId: ${config.clientId}, endpoint ${rotation.position() + 1}/${rotation.size()})")

            // Create MQTT client
            client = MqttAsyncClient(serverUri, config.clientId, MemoryPersistence())
            mqttClient = client
            client.setCallback(createMqttCallback(client, gen, endpoint))

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
            val connectingClient = client
            client.connect(options, null, object : IMqttActionListener {
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
                    val error = PNError.connectionFailed(
                        exception?.message ?: "Connection failed",
                        exception
                    )
                    handleConnectFailure(connectingClient, gen, exception, error)
                }
            })

        } catch (e: Exception) {
            Log.e(TAG, "Connection error: ${e.message}", e)
            e.cause?.let { cause ->
                Log.e(TAG, "Connection error cause: ${cause::class.java.name}: ${cause.message}")
            }
            handleConnectFailure(client, gen, e, PNError.fromException(e, "Connection failed"))
        }
    }

    private fun handleConnectFailure(
        client: MqttAsyncClient?,
        gen: Int,
        exception: Throwable?,
        error: PNError
    ) {
        if (!connectGuard.finish(gen)) {
            // A newer attempt (or close()) replaced this one
            Log.d(TAG, "Ignoring failure of an abandoned connection attempt")
            if (client != null && client !== mqttClient) shutdownQuietly(client)
            return
        }

        state = PNState.DISCONNECTED
        notifyError(error)

        if (manualDisconnect.get()) return

        val rejected = PNFailures.isRejectedByBroker(exception)
        if (rotation.onConnectFailure(rejected)) {
            Log.w(TAG, "Endpoint unreachable - trying next endpoint ${rotation.current()}")
            scheduleFailover()
        } else {
            scheduleRetry()
        }
    }

    private fun scheduleFailover() {
        cancelRetry()
        val runnable = Runnable {
            retryRunnable = null
            if (!manualDisconnect.get()) connectInternal()
        }
        retryRunnable = runnable
        mainHandler.postDelayed(runnable, FAILOVER_DELAY_MS)
    }

    /** Disconnect and close [client] without blocking the caller. Never throws. */
    private fun shutdownQuietly(client: MqttAsyncClient) {
        try {
            client.setCallback(null)
        } catch (_: Exception) {
        }
        cleanupExecutor.execute {
            try {
                client.disconnectForcibly(0, 1000, false)
            } catch (_: Exception) {
            }
            try {
                client.close(true)
            } catch (_: Exception) {
            }
        }
    }

    private fun createMqttCallback(
        client: MqttAsyncClient,
        gen: Int,
        endpoint: PNEndpoint
    ) = object : MqttCallbackExtended {
        override fun connectComplete(reconnect: Boolean, serverURI: String?) {
            if (!connectGuard.finish(gen)) {
                Log.d(TAG, "Abandoned client connected to $serverURI - closing it")
                shutdownQuietly(client)
                return
            }

            Log.d(TAG, "Connected to $serverURI (reconnect: $reconnect)")

            state = PNState.CONNECTED
            resetRetryState()
            rotation.onConnected()
            connectedEndpoint = endpoint
            notifyStateChange(PNState.CONNECTED)
            notifyConnected()
            notifyEndpointConnected(endpoint)

            // Resubscribe to active channels (needed because cleanSession=true)
            resubscribeChannels()
        }

        override fun connectionLost(cause: Throwable?) {
            if (!connectGuard.isCurrent(gen)) {
                Log.d(TAG, "Ignoring connection loss of an abandoned client")
                return
            }
            Log.e(TAG, "Connection lost: ${cause?.message}")

            state = PNState.DISCONNECTED
            connectedEndpoint = null
            rotation.reset()
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
            Log.d(TAG, "messageArrived() called: topic=$topic, payloadSize=${message?.payload?.size}")
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
        if (activeChannels.isEmpty()) {
            Log.d(TAG, "resubscribeChannels() - no active channels to resubscribe")
            return
        }

        val channels = activeChannels.toTypedArray()
        val qos = IntArray(channels.size) { PNDeliveryMode.RELIABLE.qos }

        Log.d(TAG, "Resubscribing to ${channels.size} channels: ${channels.toList()}")

        try {
            mqttClient?.subscribe(channels, qos, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken?) {
                    Log.d(TAG, "Resubscribed to ${channels.size} channels successfully")
                }

                override fun onFailure(token: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Resubscribe failed: ${exception?.message}")
                    exception?.cause?.let {
                        Log.e(TAG, "Resubscribe failure cause: ${it::class.java.name}: ${it.message}")
                    }
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

    private fun calculateRetryDelay(attempt: Int): Long =
        PNBackoff.delay(attempt, config.reconnectDelay, config.maxReconnectDelay, Math.random())

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

        cancelRetry()
        isRetrying.set(true)
        val runnable = Runnable {
            retryRunnable = null
            if (manualDisconnect.get()) return@Runnable
            val newAttempt = retryAttempt.incrementAndGet()
            Log.d(TAG, "Executing retry attempt $newAttempt")
            connectInternal()
        }
        retryRunnable = runnable
        mainHandler.postDelayed(runnable, delayMs)
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

    private fun notifyEndpointConnected(endpoint: PNEndpoint) {
        mainHandler.post {
            endpointListeners.forEach { it.onEndpointConnected(endpoint) }
        }
    }

    private fun notifyError(error: PNError) {
        mainHandler.post {
            errorListeners.forEach { it.onError(error) }
        }
    }
}
