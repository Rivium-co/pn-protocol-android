package co.rivium.protocol

/**
 * Error from PN Protocol operations
 */
data class PNError(
    val code: Code,
    val message: String,
    val cause: Throwable? = null
) {
    enum class Code(val value: Int) {
        UNKNOWN(0),
        CONNECTION_FAILED(100),
        CONNECTION_LOST(101),
        CONNECTION_TIMEOUT(102),
        AUTH_FAILED(200),
        AUTH_EXPIRED(201),
        STREAM_FAILED(300),
        DETACH_FAILED(301),
        DISPATCH_FAILED(400),
        INVALID_CONFIG(500),
        INVALID_MESSAGE(501),
        NOT_CONNECTED(600)
    }

    override fun toString(): String = "PNError(code=$code, message='$message')"

    companion object {
        @JvmStatic
        fun connectionFailed(message: String, cause: Throwable? = null) =
            PNError(Code.CONNECTION_FAILED, message, cause)

        @JvmStatic
        fun connectionLost(message: String, cause: Throwable? = null) =
            PNError(Code.CONNECTION_LOST, message, cause)

        @JvmStatic
        fun authFailed(message: String, cause: Throwable? = null) =
            PNError(Code.AUTH_FAILED, message, cause)

        @JvmStatic
        fun notConnected() = PNError(Code.NOT_CONNECTED, "Not connected. Call open() first.")

        @JvmStatic
        fun fromException(e: Exception, defaultMessage: String = "Unknown error") =
            PNError(Code.UNKNOWN, e.message ?: defaultMessage, e)
    }
}

/**
 * Exception thrown by PN Protocol
 */
class PNException(
    message: String,
    val error: PNError? = null,
    cause: Throwable? = null
) : Exception(message, cause) {

    constructor(error: PNError) : this(error.message, error, error.cause)
}
