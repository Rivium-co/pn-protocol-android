package co.rivium.protocol

import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/**
 * Closing a client must never throw on one of the client library's own
 * threads: nothing can catch that, and the app is killed.
 *
 * The case that did it in the field: the connection was closed while it was
 * still being opened (the user signed out a moment after signing in). The
 * graceful disconnect kept a background thread waiting for the unanswered
 * connect, the client was force-closed underneath it, and the thread crashed
 * when its wait ended.
 */
class PNClientTeardownTest {

    private val uncaught = CopyOnWriteArrayList<Throwable>()
    private var previousHandler: Thread.UncaughtExceptionHandler? = null
    private lateinit var server: ServerSocket
    private val accepted = CopyOnWriteArrayList<Socket>()

    @Before
    fun setUp() {
        previousHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught.add(e) }
    }

    @After
    fun tearDown() {
        Thread.setDefaultUncaughtExceptionHandler(previousHandler)
        accepted.forEach { runCatching { it.close() } }
        runCatching { server.close() }
    }

    /** A server that accepts the TCP connection and, if [answerConnect], the MQTT connect. */
    private fun startServer(answerConnect: Boolean) {
        server = ServerSocket(0)
        thread(isDaemon = true) {
            while (!server.isClosed) {
                val socket = try { server.accept() } catch (_: Exception) { break }
                accepted.add(socket)
                if (answerConnect) {
                    thread(isDaemon = true) {
                        runCatching {
                            socket.getInputStream().read(ByteArray(256)) // the CONNECT packet
                            socket.getOutputStream().apply {
                                write(byteArrayOf(0x20, 0x02, 0x00, 0x00)) // CONNACK, accepted
                                flush()
                            }
                            // Then say nothing more, like a server that went quiet.
                            while (socket.getInputStream().read(ByteArray(256)) >= 0) { /* drain */ }
                        }
                    }
                }
            }
        }
    }

    private fun client() = MqttAsyncClient("tcp://127.0.0.1:${server.localPort}", "teardown-test", MemoryPersistence())

    private fun options() = MqttConnectOptions().apply {
        isCleanSession = true
        connectionTimeout = 30
        isAutomaticReconnect = false
    }

    @Test
    fun `force-closing during a graceful disconnect is what crashes`() {
        // Documents the hazard the teardown avoids, and proves this test setup
        // can see a crash on a library thread.
        startServer(answerConnect = false)
        val client = client()
        client.connect(options())
        Thread.sleep(300)

        // The old teardown, step by step:
        client.disconnect(1_500) // background thread waits for the unanswered connect
        Thread.sleep(100)
        runCatching { client.disconnectForcibly(0, 1_000, false) } // marks it disconnected
        client.close(true) // ...so this goes through, underneath the waiting thread

        Thread.sleep(3_000)
        assertTrue("expected the library thread to throw", uncaught.isNotEmpty())
    }

    @Test
    fun `dropping a client that is still connecting does not crash`() {
        startServer(answerConnect = false)
        val client = client()
        client.connect(options())
        Thread.sleep(300)

        PNClientTeardown.force(client)

        Thread.sleep(2_500)
        assertTrue("uncaught: $uncaught", uncaught.isEmpty())
    }

    @Test
    fun `dropping the same client twice does not crash`() {
        // close() and a late "connected" callback can both tear a client down.
        startServer(answerConnect = false)
        val client = client()
        client.connect(options())
        Thread.sleep(300)

        PNClientTeardown.force(client)
        PNClientTeardown.force(client)

        Thread.sleep(2_500)
        assertTrue("uncaught: $uncaught", uncaught.isEmpty())
    }

    @Test
    fun `a connected client closes gracefully`() {
        startServer(answerConnect = true)
        val client = client()
        client.connect(options()).waitForCompletion(5_000)
        assertTrue(client.isConnected)

        assertTrue(PNClientTeardown.graceful(client))

        Thread.sleep(1_000)
        assertTrue("uncaught: $uncaught", uncaught.isEmpty())
    }

    @Test
    fun `a drop waits out a graceful disconnect that is still running`() {
        // The one way the new teardown has a background disconnect alive when
        // it drops a client: the graceful close timed out. Its wait is short
        // (GRACEFUL_QUIESCE_MS), and the drop only runs after
        // SETTLE_AFTER_GRACEFUL_MS, so the thread is gone by then.
        assertTrue(PNClientTeardown.SETTLE_AFTER_GRACEFUL_MS > PNClientTeardown.GRACEFUL_QUIESCE_MS * 4)

        startServer(answerConnect = false)
        val client = client()
        client.connect(options())
        Thread.sleep(300)

        client.disconnect(PNClientTeardown.GRACEFUL_QUIESCE_MS)
        PNClientTeardown.force(client, PNClientTeardown.SETTLE_AFTER_GRACEFUL_MS)

        Thread.sleep(2_000)
        assertTrue("uncaught: $uncaught", uncaught.isEmpty())
    }

    @Test
    fun `a graceful close that fails falls back to dropping without a crash`() {
        // Not connected: the graceful path refuses, the caller drops the client.
        startServer(answerConnect = false)
        val client = client()
        client.connect(options())
        Thread.sleep(300)

        if (!PNClientTeardown.graceful(client)) {
            PNClientTeardown.force(client, PNClientTeardown.SETTLE_AFTER_GRACEFUL_MS)
        }

        Thread.sleep(2_500)
        assertTrue("uncaught: $uncaught", uncaught.isEmpty())
    }
}
