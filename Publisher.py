import pandas as pd
import time
import paho.mqtt.client as mqtt


MQTT_CLIENT_ID="Publisher"
HOST="localhost"
MQTT_TOPIC="tower_6/sensor"
client=mqtt.Client("Publisher")

client.connect(host=HOST,port=1883, keepalive=260)


with open("/home/virtualmachine1/PycharmProjects/mqtt_anomalyDetection/SensorData.txt") as file:
    for line in file:
        print(line.rstrip())
        client.publish(MQTT_TOPIC, line.rstrip())
        time.sleep(8)
