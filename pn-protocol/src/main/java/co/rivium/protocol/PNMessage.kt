package co.rivium.protocol

import java.util.UUID

/**
 * Message in PN Protocol
 *
 * Wraps MQTT message with PN Protocol terminology.
 */
data class PNMessage(
    /** Target channel (MQTT: topic) */
    val channel: String,

    /** Message payload as bytes */
    val payload: ByteArray,

    /** Delivery guarantee mode (MQTT: QoS) */
    val mode: PNDeliveryMode = PNDeliveryMode.RELIABLE,

    /** Persist message for new subscribers (MQTT: retain) */
    val persist: Boolean = false,

    /** Message timestamp */
    val timestamp: Long = System.currentTimeMillis(),

    /** Unique message ID */
    val id: String = UUID.randomUUID().toString()
) {
    /** Get payload as UTF-8 string */
    fun payloadAsString(): String = String(payload, Charsets.UTF_8)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as PNMessage
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String = "PNMessage(id='$id', channel='$channel', mode=$mode, size=${payload.size})"

    /**
     * Builder for PNMessage
     */
    class Builder {
        private var channel: String = ""
        private var payload: ByteArray = byteArrayOf()
        private var mode: PNDeliveryMode = PNDeliveryMode.RELIABLE
        private var persist: Boolean = false

        fun channel(channel: String) = apply { this.channel = channel }
        fun payload(payload: ByteArray) = apply { this.payload = payload }
        fun payload(payload: String) = apply { this.payload = payload.toByteArray(Charsets.UTF_8) }
        fun mode(mode: PNDeliveryMode) = apply { this.mode = mode }
        fun persist(persist: Boolean) = apply { this.persist = persist }

        fun build(): PNMessage {
            require(channel.isNotBlank()) { "Channel is required" }
            return PNMessage(channel, payload, mode, persist)
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()

        /** Create a simple text message */
        @JvmStatic
        fun text(channel: String, text: String) = PNMessage(
            channel = channel,
            payload = text.toByteArray(Charsets.UTF_8)
        )

        /** Create from MQTT message */
        @JvmStatic
        internal fun fromMqtt(topic: String, payload: ByteArray, qos: Int, retained: Boolean) = PNMessage(
            channel = topic,
            payload = payload,
            mode = PNDeliveryMode.fromQos(qos),
            persist = retained
        )
    }
}
