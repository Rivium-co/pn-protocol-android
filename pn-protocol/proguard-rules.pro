# PN Protocol ProGuard Rules

# Keep public API
-keep class co.rivium.protocol.** { *; }

# Keep MQTT client
-keep class org.eclipse.paho.client.mqttv3.** { *; }
