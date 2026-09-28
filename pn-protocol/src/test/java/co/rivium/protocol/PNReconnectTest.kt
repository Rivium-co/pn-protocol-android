package co.rivium.protocol

import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttSecurityException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLHandshakeException

class PNBackoffTest {

    private val base = 1_000L
    private val max = 60_000L

    @Test
    fun `defaults are 30s keepalive, 60s cap and unlimited attempts`() {
        val config = PNConfig.builder().gateway("gw").clientId("c").build()
        assertEquals(30, config.heartbeatInterval)
        assertEquals(60_000L, config.maxReconnectDelay)
        assertEquals(1_000L, config.reconnectDelay)
        assertEquals(0, config.maxReconnectAttempts)
    }

    @Test
    fun `starts at 1s and doubles without jitter at the midpoint`() {
        assertEquals(1_000L, PNBackoff.delay(0, base, max, 0.5))
        assertEquals(2_000L, PNBackoff.delay(1, base, max, 0.5))
        assertEquals(4_000L, PNBackoff.delay(2, base, max, 0.5))
        assertEquals(32_000L, PNBackoff.delay(5, base, max, 0.5))
    }

    @Test
    fun `delay is capped at the max`() {
        for (attempt in 6..200) {
            assertEquals(max, PNBackoff.delay(attempt, base, max, 0.5))
        }
    }

    @Test
    fun `jitter stays within plus minus 20 percent`() {
        assertEquals(800L, PNBackoff.delay(0, base, max, 0.0))
        assertEquals(1_200L, PNBackoff.delay(0, base, max, 1.0))
        assertEquals(48_000L, PNBackoff.delay(50, base, max, 0.0))
        assertEquals(72_000L, PNBackoff.delay(50, base, max, 1.0))

        repeat(10_000) { i ->
            val attempt = i % 40
            val d = PNBackoff.delay(attempt, base, max, Math.random())
            val nominal = minOf(base * (1L shl minOf(attempt, 20)), max)
            assertTrue("attempt $attempt delay $d", d >= (nominal * 0.8).toLong())
            assertTrue("attempt $attempt delay $d", d <= (nominal * 1.2).toLong())
            assertTrue(d <= 72_000L)
        }
    }

    @Test
    fun `huge attempt counts do not overflow`() {
        val d = PNBackoff.delay(Int.MAX_VALUE, base, max, 0.5)
        assertEquals(max, d)
        assertTrue(PNBackoff.delay(-5, base, max, 0.5) == base)
    }
}

class PNFailuresTest {

    @Test
    fun `bad credentials and not authorized are broker rejections`() {
        assertTrue(PNFailures.isRejectedByBroker(
            MqttSecurityException(MqttException.REASON_CODE_FAILED_AUTHENTICATION.toInt())))
        assertTrue(PNFailures.isRejectedByBroker(
            MqttSecurityException(MqttException.REASON_CODE_NOT_AUTHORIZED.toInt())))
        assertTrue(PNFailures.isRejectedByBroker(
            MqttException(MqttException.REASON_CODE_INVALID_CLIENT_ID.toInt())))
        // wrapped
        assertTrue(PNFailures.isRejectedByBroker(RuntimeException(
            MqttSecurityException(MqttException.REASON_CODE_NOT_AUTHORIZED.toInt()))))
    }

    @Test
    fun `network errors are not broker rejections`() {
        assertFalse(PNFailures.isRejectedByBroker(null))
        assertFalse(PNFailures.isRejectedByBroker(
            MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR.toInt(), ConnectException("refused"))))
        assertFalse(PNFailures.isRejectedByBroker(
            MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT.toInt())))
        assertFalse(PNFailures.isRejectedByBroker(
            MqttException(MqttException.REASON_CODE_SERVER_CONNECT_ERROR.toInt(), SSLHandshakeException("bad cert"))))
        assertFalse(PNFailures.isRejectedByBroker(SocketTimeoutException("timeout")))
        assertFalse(PNFailures.isRejectedByBroker(
            MqttException(MqttException.REASON_CODE_BROKER_UNAVAILABLE.toInt())))
    }
}

class PNEndpointRotationTest {

    private val a = PNEndpoint("a.example", 443, true)
    private val b = PNEndpoint("b.example", 8883, true)
    private val c = PNEndpoint("c.example", 1883, false)

    @Test
    fun `uri keeps ssl and tcp schemes`() {
        assertEquals("ssl://a.example:443", a.uri)
        assertEquals("tcp://c.example:1883", c.uri)
    }

    @Test
    fun `fails over to the next endpoint on network failure`() {
        val r = PNEndpointRotation()
        r.startRound(listOf(a, b, c))
        assertEquals(a, r.current())
        assertTrue(r.onConnectFailure(rejectedByBroker = false))
        assertEquals(b, r.current())
        assertTrue(r.onConnectFailure(rejectedByBroker = false))
        assertEquals(c, r.current())
        // last endpoint failed: round over, caller backs off
        assertFalse(r.onConnectFailure(rejectedByBroker = false))
        assertFalse(r.inRound())
        // next round starts again at the first endpoint
        r.startRound(listOf(a, b, c))
        assertEquals(a, r.current())
    }

    @Test
    fun `does not fail over when the broker rejects the credentials`() {
        val r = PNEndpointRotation()
        r.startRound(listOf(a, b, c))
        assertFalse(r.onConnectFailure(rejectedByBroker = true))
        assertFalse(r.inRound())
        assertNull(r.current())
    }

    @Test
    fun `single endpoint behaves like before - back off after each failure`() {
        val r = PNEndpointRotation()
        r.startRound(listOf(a))
        assertFalse(r.onConnectFailure(rejectedByBroker = false))
    }

    @Test
    fun `success reports the winning endpoint and ends the round`() {
        val r = PNEndpointRotation()
        r.startRound(listOf(a, b))
        r.onConnectFailure(false)
        assertEquals(b, r.onConnected())
        assertFalse(r.inRound())
    }

    @Test
    fun `duplicates are tried once`() {
        val r = PNEndpointRotation()
        r.startRound(listOf(a, a, b))
        assertEquals(2, r.size())
    }
}

class PNConnectGuardTest {

    @Test
    fun `second attempt is refused while one is in flight`() {
        val g = PNConnectGuard()
        val first = g.tryBegin()
        assertNotNull(first)
        assertNull(g.tryBegin())
        assertTrue(g.finish(first!!))
        assertNotNull(g.tryBegin())
    }

    @Test
    fun `abandoned attempt cannot release a newer one`() {
        val g = PNConnectGuard()
        val old = g.tryBegin()!!
        g.invalidate()                 // e.g. forced reconnect after a network change
        val fresh = g.tryBegin()!!
        assertFalse(g.isCurrent(old))
        assertFalse(g.finish(old))     // late callback of the old client
        assertTrue(g.isInFlight())     // fresh attempt still guarded
        assertNull(g.tryBegin())
        assertTrue(g.finish(fresh))
    }

    @Test
    fun `concurrent triggers start exactly one connect`() {
        val g = PNConnectGuard()
        val started = AtomicInteger(0)
        val pool = Executors.newFixedThreadPool(8)
        val go = CountDownLatch(1)
        val done = CountDownLatch(50)
        repeat(50) {
            pool.execute {
                go.await()
                if (g.tryBegin() != null) started.incrementAndGet()
                done.countDown()
            }
        }
        go.countDown()
        assertTrue(done.await(5, TimeUnit.SECONDS))
        pool.shutdown()
        assertEquals(1, started.get())
    }
}
