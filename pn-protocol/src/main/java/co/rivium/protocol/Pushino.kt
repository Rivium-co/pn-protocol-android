package co.rivium.protocol

/**
 * @deprecated Use PNSocket directly instead. The singleton pattern causes conflicts
 * when multiple SDKs (e.g. RiviumPush + RiviumSync) use pn-protocol in the same app.
 *
 * Migration:
 * ```kotlin
 * // Before:
 * Pushino.initialize(config)
 * Pushino.socket().open()
 *
 * // After:
 * val socket = PNSocket(config)
 * socket.open()
 * ```
 */
@Deprecated("Use PNSocket(config) directly. Singleton causes conflicts in multi-SDK apps.")
object Pushino {

    private var socket: PNSocket? = null
    private var config: PNConfig? = null

    const val VERSION = "1.0.0"
    const val PROTOCOL = "PN/1.0"

    @JvmStatic
    @Deprecated("Use PNSocket(config) directly", ReplaceWith("PNSocket(config)"))
    fun initialize(config: PNConfig): Pushino {
        socket?.close()
        this.config = config
        this.socket = PNSocket(config)
        return this
    }

    @JvmStatic
    @Deprecated("Use PNSocket(config) directly and keep your own reference")
    fun socket(): PNSocket {
        return socket ?: throw PNException("PNProtocol not initialized. Call initialize() first.")
    }

    @JvmStatic
    @Deprecated("Use socket.close() directly")
    fun isInitialized(): Boolean = socket != null

    @JvmStatic
    @Deprecated("Use socket.close() directly")
    fun shutdown() {
        socket?.close()
        socket = null
        config = null
    }

    @JvmStatic
    @Deprecated("Use PNSocket(config) directly and keep your own config reference")
    fun config(): PNConfig? = config
}
