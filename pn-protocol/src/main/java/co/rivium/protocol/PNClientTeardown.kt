package co.rivium.protocol

import org.eclipse.paho.client.mqttv3.MqttAsyncClient

/**
 * Stops a client for good without ever crashing the app.
 *
 * The client library's graceful disconnect runs on its own background thread
 * and can keep waiting (30 s by default) for work in flight - a connect that
 * has not been answered, for instance. Closing the client while that thread
 * is still alive makes it throw on its own thread, which nothing can catch:
 * the process dies. So:
 *
 *  - [graceful] is only for a client that is connected, uses a short wait, and
 *    closes only once the disconnect has really finished;
 *  - everything else goes through [force], which disconnects inline (no
 *    background thread) and never force-closes.
 */
internal object PNClientTeardown {

    /** How long a graceful disconnect lets in-flight messages finish. */
    const val GRACEFUL_QUIESCE_MS = 250L

    /** How long to wait for the graceful disconnect itself. */
    const val GRACEFUL_WAIT_MS = 3_000L

    /**
     * Let a graceful disconnect that timed out finish before [force] runs.
     * Must stay well above [GRACEFUL_QUIESCE_MS]: [force] closes the client,
     * and that is only safe once the graceful disconnect's thread has ended.
     */
    const val SETTLE_AFTER_GRACEFUL_MS = 2_000L

    /**
     * Say goodbye to the server and close. Blocks up to [GRACEFUL_WAIT_MS].
     * Returns false when it did not complete; the caller then uses [force]
     * with [SETTLE_AFTER_GRACEFUL_MS].
     */
    fun graceful(client: MqttAsyncClient): Boolean {
        return try {
            client.disconnect(GRACEFUL_QUIESCE_MS).waitForCompletion(GRACEFUL_WAIT_MS)
            client.close()
            true
        } catch (_: Exception) {
            false
        }
    }

    /** Drop the connection without a goodbye and close. Blocking; never throws. */
    fun force(client: MqttAsyncClient, settleMs: Long = 0) {
        if (settleMs > 0) {
            try {
                Thread.sleep(settleMs)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        try {
            client.setCallback(null)
        } catch (_: Exception) {
        }
        try {
            client.disconnectForcibly(0, 1000, false)
        } catch (_: Exception) {
        }
        try {
            // Never close(true): see the class comment.
            client.close()
        } catch (_: Exception) {
        }
    }
}
