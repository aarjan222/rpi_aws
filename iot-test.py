import time
import paho.mqtt.client as mqtt
import ssl
import json

# Global connection flag for MQTT connection status
flag_connected = 0

# AWS IoT Core connection setup
def setup_aws_client():
    def aws_on_connect(client, userdata, flags, rc):
        print(f"Connected to AWS IoT: {rc}")
        # subscribe data on topic raspi/control
        client.subscribe("aws/control")

    # Callback function for AWS broadcast messages
    def aws_broadcast(client, userdata, msg):
        print(f"RPi Broadcast message: {msg.payload.decode('utf-8')}")
    
    aws_client = mqtt.Client()
    aws_client.on_connect = aws_on_connect
    aws_client.message_callback_add('aws/control', aws_broadcast)

    aws_client.tls_set(
        ca_certs='./rootCA.pem',
        certfile='./certificate.pem.crt',
        keyfile='./private.pem.key',
        tls_version=ssl.PROTOCOL_SSLv23
    )
    aws_client.tls_insecure_set(True)
    aws_client.connect("a395t3u3i1xd3m-ats.iot.us-east-1.amazonaws.com", 8883, 60)

    aws_client.loop_start()
    return aws_client


# ESP32 and Raspberry Pi communication setup
def setup_local_client(aws_client):
    def on_connect(client, userdata, flags, rc):
        global flag_connected
        flag_connected = 1
        print("Connected to local MQTT server")
        client_subscriptions(client)

    def on_disconnect(client, userdata, rc):
        global flag_connected
        flag_connected = 0
        print("Disconnected from local MQTT server")

    # Callback function for ESP32 sensor data
    def callback_esp32_sensor1(client, userdata, msg):
        sensor_data = msg.payload.decode('utf-8')
        print(f"ESP32 Sensor1 data: {sensor_data}")
        
        # Publish data to AWS IoT Core
        aws_client.publish(
            "raspi/data",
            payload=json.dumps({"msg": sensor_data}),
            qos=0,
            retain=False
        )

    # Callback function for RPi broadcast messages
    def callback_rpi_broadcast(client, userdata, msg):
        print(f"RPi Broadcast message: {msg.payload.decode('utf-8')}")

    # Subscribe to relevant topics
    def client_subscriptions(client):
        client.subscribe("esp32/#")
        client.subscribe("rpi/broadcast")

    # Set up the local MQTT client
    local_client = mqtt.Client("rpi_client1")  # Ensure unique client ID
    local_client.on_connect = on_connect
    local_client.on_disconnect = on_disconnect
    local_client.message_callback_add('esp32/sensor1', callback_esp32_sensor1)
    local_client.message_callback_add('rpi/broadcast', callback_rpi_broadcast)
    local_client.connect('127.0.0.1', 1883)

    local_client.loop_start()
    return local_client


# Main function to manage communication
def main():
    # Set up AWS IoT Client
    aws_client = setup_aws_client()

    # Set up local MQTT client for Raspberry Pi and ESP32 communication
    local_client = setup_local_client(aws_client)

    # Main loop to check connection status and keep program alive
    while True:
        time.sleep(4)
        if not flag_connected:
            print("Attempting to reconnect to local MQTT server...")


if __name__ == "__main__":
    main()
