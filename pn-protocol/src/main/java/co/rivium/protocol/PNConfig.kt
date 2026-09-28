package co.rivium.protocol

/**
 * Configuration for PNSocket connection
 *
 * | PN Protocol        | MQTT                  |
 * |--------------------|-----------------------|
 * | gateway            | broker host           |
 * | heartbeatInterval  | keepAliveInterval     |
 * | freshStart         | cleanSession          |
 * | exitSignal         | lastWillAndTestament  |
 */
data class PNConfig(
    /** Gateway server host (MQTT: broker) */
    val gateway: String,

    /** Gateway server port */
    val port: Int = 1883,

    /** Unique client identifier */
    val clientId: String,

    /** Authentication credentials */
    val auth: PNAuth? = null,

    /** Keep connection alive interval in seconds (MQTT: keepAliveInterval) */
    val heartbeatInterval: Int = 30,

    /** Connection timeout in seconds */
    val connectionTimeout: Int = 30,

    /** Start with fresh state, ignore persisted data (MQTT: cleanSession) */
    val freshStart: Boolean = true,

    /** Auto-reconnect on connection loss */
    val autoReconnect: Boolean = true,

    /** Maximum reconnect attempts (0 = infinite) */
    val maxReconnectAttempts: Int = 0,

    /** Initial reconnect delay in milliseconds */
    val reconnectDelay: Long = 1000,

    /** Maximum reconnect delay in milliseconds (a ±20 % jitter is applied) */
    val maxReconnectDelay: Long = 60000,

    /** Exit signal - message sent on unexpected disconnect (MQTT: Last Will) */
    val exitSignal: PNExitSignal? = null,

    /** Enable secure connection (TLS/SSL) */
    val secure: Boolean = false
) {
    /**
     * Builder for PNConfig
     */
    class Builder {
        private var gateway: String = ""
        private var port: Int = 1883
        private var clientId: String = ""
        private var auth: PNAuth? = null
        private var heartbeatInterval: Int = 30
        private var connectionTimeout: Int = 30
        private var freshStart: Boolean = true
        private var autoReconnect: Boolean = true
        private var maxReconnectAttempts: Int = 0
        private var reconnectDelay: Long = 1000
        private var maxReconnectDelay: Long = 60000
        private var exitSignal: PNExitSignal? = null
        private var secure: Boolean = false

        fun gateway(gateway: String) = apply { this.gateway = gateway }
        fun port(port: Int) = apply { this.port = port }
        fun clientId(clientId: String) = apply { this.clientId = clientId }
        fun auth(auth: PNAuth) = apply { this.auth = auth }
        fun heartbeatInterval(seconds: Int) = apply { this.heartbeatInterval = seconds }
        fun connectionTimeout(seconds: Int) = apply { this.connectionTimeout = seconds }
        fun freshStart(fresh: Boolean) = apply { this.freshStart = fresh }
        fun autoReconnect(auto: Boolean) = apply { this.autoReconnect = auto }
        fun maxReconnectAttempts(max: Int) = apply { this.maxReconnectAttempts = max }
        fun reconnectDelay(delay: Long) = apply { this.reconnectDelay = delay }
        fun maxReconnectDelay(delay: Long) = apply { this.maxReconnectDelay = delay }
        fun exitSignal(signal: PNExitSignal) = apply { this.exitSignal = signal }
        fun secure(secure: Boolean) = apply { this.secure = secure }

        fun build(): PNConfig {
            require(gateway.isNotBlank()) { "Gateway is required" }
            require(clientId.isNotBlank()) { "Client ID is required" }
            return PNConfig(
                gateway, port, clientId, auth, heartbeatInterval, connectionTimeout,
                freshStart, autoReconnect, maxReconnectAttempts, reconnectDelay,
                maxReconnectDelay, exitSignal, secure
            )
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }
}

/**
 * Authentication options
 */
sealed class PNAuth {
    /** Username/password authentication */
    data class Basic(val username: String, val password: String) : PNAuth()

    /** Token-based authentication */
    data class Token(val token: String) : PNAuth()

    companion object {
        @JvmStatic
        fun basic(username: String, password: String) = Basic(username, password)

        @JvmStatic
        fun token(token: String) = Token(token)
    }
}

/**
 * Exit signal sent when client disconnects unexpectedly (MQTT: Last Will and Testament)
 */
data class PNExitSignal(
    /** Channel to send exit signal to */
    val channel: String,

    /** Payload to send */
    val payload: ByteArray,

    /** Delivery mode */
    val mode: PNDeliveryMode = PNDeliveryMode.RELIABLE,

    /** Persist the exit signal */
    val persist: Boolean = false
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as PNExitSignal
        return channel == other.channel && payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int = 31 * channel.hashCode() + payload.contentHashCode()

    companion object {
        @JvmStatic
        fun create(channel: String, payload: String) = PNExitSignal(
            channel = channel,
            payload = payload.toByteArray(Charsets.UTF_8)
        )
    }
}
