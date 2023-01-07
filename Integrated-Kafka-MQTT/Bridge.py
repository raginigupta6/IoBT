import paho.mqtt.client as mqtt
from KafkaUtils import *
import time
import time
import pandas as pd
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import NamedTuple
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


TOKEN = "Fdh-kOfYFmROa9moZQeprDTKfP20WRIk8ZKbrYeyTVkc_DViQyOm0fRW-hBoY6nN3Q83D7BlCiW30AlYivL6KA=="
ORG = "UIUC"
BUCKET = "MSAdata"
HOST="localhost"
MQTT_PORT=1883
INFLUXDB_CLIENT = InfluxDBClient(url="http://localhost:8086", token=TOKEN)
write_api = INFLUXDB_CLIENT.write_api(write_options=SYNCHRONOUS)
mqtt_broker='192.168.250.51' # or 192.168.250.51
mqtt_client=mqtt.Client('MQTTBridge')
mqtt_client.connect(mqtt_broker)
MQTT_TOPIC = [(f"dvpg/msa/tower/1/height/1000/sonic", 0), (f"dvpg/msa/tower/2/height/1000/sonic", 0),
              (f"dvpg/msa/tower/4/height/1000/sonic", 0), (f"dvpg/msa/tower/6/height/1000/sonic", 0),
              (f"dvpg/msa/tower/7/height/1000/sonic", 0), (f"dvpg/msa/tower/8/height/1000/sonic", 0),
              (f"dvpg/msa/tower/9/height/1000/sonic", 0), (f"dvpg/msa/tower/10/height/1000/sonic", 0),
              (f"dvpg/msa/tower/11/height/1000/sonic", 0)]



# Insert Logic To Check which Kafka Broker is up and then send the data to it
kafka_broker="192.168.32.151" # IP address
kafka_port="9092"
kafkaInstance = KafkaUtils(broker = kafka_broker, port = kafka_port)
kafka_topic="msaDataTower1Sonic"  ## Where Kafka wants to publish
#kafkaInstance.createTopic(kafka_topic)



class SonicSensorData(NamedTuple):
    timestamp: str
    latitude: str
    longitude: str
    Altitude: float
    Tower: str
    sensorType: str
    measurement:str
    measurement_value_1: float
    measurement_value_2: float
    measurement_value_3: float
    measurement_value_4: float
    measurement_value_5: float

def parse_mqtt_message(payload):
    #print("Data payload is", str(payload))
    res= json.loads(payload)
    time_stamp=res['time']

    location=res["location"]
    latitude=location['lat']
    longitude=location['lon']
    altitude=location['alt']['value']
    tower=res['tower']
    sensorType=res['sensorType']
    reading=res['reading']
    if sensorType=='soil':
        measurement='12FebSonicKafka' ## Corresponds to Table Name
        measurement_value1=reading[0]['value']
        measurement_value2=reading[1]['value']
        measurement_value3=reading[2]['value']
        measurement_value4=reading[3]['value']
        measurement_value5=reading[4]['value']
        return SonicSensorData(time_stamp, latitude, longitude, altitude, tower, sensorType,measurement,measurement_value1,measurement_value2,measurement_value3,measurement_value4,measurement_value5)


def send_sensor_data_to_influx(sensor_data):
    if sensor_data.sensorType=='soil':
        json_body = [
                {
                    'measurement': sensor_data.measurement,
                    'tags': {
                        'latitude': sensor_data.latitude,
                        'longitude': sensor_data.longitude,
                        'Time_stamp': sensor_data.timestamp,
                        'tower': sensor_data.Tower,
                        'altitude': sensor_data.Altitude,
                        'sensorType': sensor_data.sensorType

                    },
                    'fields': {
                        'measurement_value1': sensor_data.measurement_value_1,
                        'measurement_value2': sensor_data.measurement_value_2,
                        'measurement_value3': sensor_data.measurement_value_3,
                        'measurement_value4': sensor_data.measurement_value_4,
                        'measurement_value5': sensor_data.measurement_value_5

                    }
                }
        ]
        write_api.write(BUCKET, ORG, json_body)
        #print("Data uploaded to InfluxDB")
def on_message(client,userdata,message):

    msg_payload=str(message.payload)
    #print("Received MQTT message", msg_payload)
    #data_to_send=message.payload.decode("utf-8")


    kafkaInstance.sendData(topic_name=kafka_topic, data=message.payload)
    #print("Kafka Published:  ", data_to_send)
    insertnflux(message.payload)


Kafka_running = True
MQTT_running = False

def insertnflux(mqtt_data):
    global Kafka_running, MQTT_running
    #print("data is", mqtt_data)
    print('Kafka_running:', Kafka_running, " MQTT_running:", MQTT_running)

    kafka_data = None

    if Kafka_running:
        kafka_data = kafkaInstance.receiveData(topic_name=kafka_topic)
        #print(kafka_data)

        if kafka_data:
            try:
                parsed_sensor_data = parse_mqtt_message(kafka_data.decode("utf-8"))
                send_sensor_data_to_influx(parsed_sensor_data)
            except:
                print('exception in kafka')
        else:
            print("Kafka is None")

    if MQTT_running and not Kafka_running:
        try:
            parsed_sensor_data = parse_mqtt_message(mqtt_data.decode("utf-8"))
            send_sensor_data_to_influx(parsed_sensor_data)
        except:
            print('exception in mqtt parsing')

# Python thread to read variables
import _thread
import time
import random
def update_global_state():
    global Kafka_running, MQTT_running
    i=0
    while i<20:
        # update the global variablee
        print('updating global varsiable:', i)
        Kafka_running = random.randint(0,1)
        MQTT_running = random.randint(0, 1)

        time.sleep(2)
        i+=1


def insertinfluxState():
    global Kafka_running, MQTT_running

    while true:
        time.sleep(2)

_thread.start_new_thread(update_global_state,())
#_thread.start_new_thread(insertinfluxState,())

mqtt_client.subscribe(MQTT_TOPIC)
mqtt_client.on_message=on_message
mqtt_client.loop_forever()

