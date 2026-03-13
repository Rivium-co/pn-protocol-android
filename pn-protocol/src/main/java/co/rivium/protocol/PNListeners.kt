package co.rivium.protocol

/**
 * Listener for connection state changes
 */
interface PNConnectionListener {
    /** Called when connection state changes */
    fun onStateChanged(state: PNState)

    /** Called when successfully connected */
    fun onConnected()

    /** Called when disconnected */
    fun onDisconnected(reason: String?)

    /** Called when reconnecting (with retry info) */
    fun onReconnecting(attempt: Int, nextRetryMs: Long)
}

/**
 * Listener for incoming messages
 */
fun interface PNMessageListener {
    fun onMessage(message: PNMessage)
}

/**
 * Listener for errors
 */
fun interface PNErrorListener {
    fun onError(error: PNError)
}

/**
 * Callback for dispatch (publish) operations
 */
interface PNDispatchCallback {
    fun onSuccess(messageId: String)
    fun onFailure(error: PNError)
}

/**
 * Simple adapter for PNConnectionListener with default empty implementations
 */
open class PNConnectionAdapter : PNConnectionListener {
    override fun onStateChanged(state: PNState) {}
    override fun onConnected() {}
    override fun onDisconnected(reason: String?) {}
    override fun onReconnecting(attempt: Int, nextRetryMs: Long) {}
}
