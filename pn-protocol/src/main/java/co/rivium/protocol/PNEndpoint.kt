package co.rivium.protocol

/**
 * One gateway address PNSocket can connect to.
 *
 * @param host Gateway host name
 * @param port Gateway port (1..65535)
 * @param secure Use TLS (ssl://) when true, plain TCP (tcp://) otherwise
 */
data class PNEndpoint(
    val host: String,
    val port: Int,
    val secure: Boolean = true
) {
    /** Connection URI, e.g. `ssl://host:8883`. */
    val uri: String get() = "${if (secure) "ssl" else "tcp"}://$host:$port"

    override fun toString(): String = uri
}

/**
 * Supplies the endpoints to try, in order, at the start of every connection round.
 * The first endpoint that accepts the connection wins; on a network failure
 * (timeout, refused, TLS error) PNSocket moves on to the next one.
 * An empty list means "use the gateway/port/secure from [PNConfig]".
 */
fun interface PNEndpointProvider {
    fun endpoints(): List<PNEndpoint>
}

/**
 * Notified (on the main thread) with the endpoint a connection was established on.
 */
fun interface PNEndpointListener {
    fun onEndpointConnected(endpoint: PNEndpoint)
}
