package co.rivium.protocol

/**
 * Connection states for PNSocket
 */
enum class PNState {
    /** Not connected to gateway */
    DISCONNECTED,

    /** Establishing connection */
    CONNECTING,

    /** Connected and ready */
    CONNECTED,

    /** Reconnecting after connection loss */
    RECONNECTING,

    /** Gracefully disconnecting */
    DISCONNECTING
}
