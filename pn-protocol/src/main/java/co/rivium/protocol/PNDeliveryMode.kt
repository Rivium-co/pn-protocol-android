package co.rivium.protocol

/**
 * Delivery guarantee modes for PN Protocol
 *
 * | Mode       | MQTT QoS | Description                    |
 * |------------|----------|--------------------------------|
 * | FAST       | QoS 0    | Fire and forget, no guarantee  |
 * | RELIABLE   | QoS 1    | At least once delivery         |
 * | EXACT_ONCE | QoS 2    | Exactly once delivery          |
 */
enum class PNDeliveryMode(val qos: Int) {
    /** Fire and forget - fastest, no guarantee (QoS 0) */
    FAST(0),

    /** At least once delivery (QoS 1) */
    RELIABLE(1),

    /** Exactly once delivery (QoS 2) */
    EXACT_ONCE(2);

    companion object {
        @JvmStatic
        fun fromQos(qos: Int): PNDeliveryMode = entries.find { it.qos == qos } ?: RELIABLE
    }
}
