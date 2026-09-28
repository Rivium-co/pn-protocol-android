package co.rivium.protocol

import org.eclipse.paho.client.mqttv3.MqttException
import kotlin.math.min
import kotlin.math.pow

/**
 * Exponential backoff with jitter: base * 2^attempt, capped at max, then ±20 %.
 */
internal object PNBackoff {
    const val MULTIPLIER = 2.0
    const val JITTER_FACTOR = 0.2
    private const val MAX_EXPONENT = 30

    /**
     * @param random a value in [0, 1] (Math.random() in production)
     */
    fun delay(attempt: Int, baseMs: Long, maxMs: Long, random: Double): Long {
        val base = baseMs.coerceAtLeast(0L)
        val max = maxMs.coerceAtLeast(base)
        val exponential = base * MULTIPLIER.pow(attempt.coerceIn(0, MAX_EXPONENT).toDouble())
        val capped = min(exponential, max.toDouble())
        val factor = 1.0 + JITTER_FACTOR * (random.coerceIn(0.0, 1.0) * 2 - 1)
        return (capped * factor).toLong().coerceAtLeast(0L)
    }
}

/**
 * Classifies connect failures.
 */
internal object PNFailures {
    private val BROKER_REJECTIONS = setOf(
        MqttException.REASON_CODE_FAILED_AUTHENTICATION.toInt(),
        MqttException.REASON_CODE_NOT_AUTHORIZED.toInt(),
        MqttException.REASON_CODE_INVALID_CLIENT_ID.toInt()
    )

    /**
     * True when the gateway was reached and refused the credentials / client id.
     * Another endpoint of the same service would refuse them too, so there is
     * no point failing over.
     */
    fun isRejectedByBroker(error: Throwable?): Boolean {
        var current: Throwable? = error
        var depth = 0
        while (current != null && depth < 5) {
            if (current is MqttException && current.reasonCode in BROKER_REJECTIONS) return true
            current = current.cause
            depth++
        }
        return false
    }
}

/**
 * Walks the endpoint list of one connection round.
 */
internal class PNEndpointRotation {
    private var endpoints: List<PNEndpoint> = emptyList()
    private var index = 0
    private var active = false

    @Synchronized
    fun inRound(): Boolean = active

    @Synchronized
    fun startRound(list: List<PNEndpoint>) {
        endpoints = list.distinct()
        index = 0
        active = endpoints.isNotEmpty()
    }

    @Synchronized
    fun current(): PNEndpoint? = if (active) endpoints.getOrNull(index) else null

    @Synchronized
    fun position(): Int = index

    @Synchronized
    fun size(): Int = endpoints.size

    /**
     * Record a failed attempt on [current].
     * @return true if the next endpoint should be tried right away; false when the
     *         round is over (all endpoints failed, or the broker rejected us) and the
     *         caller should back off. The next round starts again from the first endpoint.
     */
    @Synchronized
    fun onConnectFailure(rejectedByBroker: Boolean): Boolean {
        if (!active) return false
        if (!rejectedByBroker && index + 1 < endpoints.size) {
            index++
            return true
        }
        active = false
        return false
    }

    /** Record a successful connection; returns the endpoint that worked. */
    @Synchronized
    fun onConnected(): PNEndpoint? {
        val winner = current()
        active = false
        return winner
    }

    @Synchronized
    fun reset() {
        active = false
        index = 0
    }
}

/**
 * Ensures at most one connect attempt is in flight, and lets callbacks from an
 * abandoned client be recognised (generation no longer current) and ignored.
 */
internal class PNConnectGuard {
    private var generation = 0
    private var inFlight = false

    /** Begin an attempt. Returns its generation, or null if one is already in flight. */
    @Synchronized
    fun tryBegin(): Int? {
        if (inFlight) return null
        inFlight = true
        generation++
        return generation
    }

    /** End the attempt of [gen]. Returns false if [gen] was abandoned meanwhile. */
    @Synchronized
    fun finish(gen: Int): Boolean {
        if (gen != generation) return false
        inFlight = false
        return true
    }

    @Synchronized
    fun isCurrent(gen: Int): Boolean = gen == generation

    @Synchronized
    fun current(): Int = generation

    @Synchronized
    fun isInFlight(): Boolean = inFlight

    /** Abandon the current attempt/connection: its callbacks become stale. */
    @Synchronized
    fun invalidate() {
        generation++
        inFlight = false
    }
}
