import pandas as pd
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import NamedTuple
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
TOKEN = "3LFzPG-Yu6fcwgNbIs87lxKZMQxhMH0tGipNErTxw1xakvLyf4HhQu7leHKftViPUpQInc1VOrcL9mZCtKYxgQ=="
ORG = "bootcamp"
BUCKET = "database3"
HOST="localhost"
MQTT_PORT=1883
ID="Subscriber"
MQTT_TOPIC="tower_6/sensor"
INFLUXDB_CLIENT = InfluxDBClient(url="http://localhost:8086", token=TOKEN)
write_api = INFLUXDB_CLIENT.write_api(write_options=SYNCHRONOUS)

class SoilSensorData(NamedTuple):
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

class TempHumiditySensorData(NamedTuple):
    timestamp: str
    latitude: str
    longitude: str
    Altitude: float
    Tower: str
    sensorType: str
    measurement:str
    measurement_value_1: float
    measurement_value_2: float

class SolarPressureSensorData(NamedTuple):
    timestamp: str
    latitude: str
    longitude: str
    Altitude: float
    Tower: str
    sensorType: str
    measurement:str
    measurement_value_1: float

def parse_mqtt_message(topic,payload):
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
        measurement='soiltemp'
        measurement_value1=reading[0]['value']
        measurement_value2=reading[1]['value']
        measurement_value3=reading[2]['value']
        measurement_value4=reading[3]['value']
        measurement_value5=reading[4]['value']
        return SoilSensorData(time_stamp, latitude, longitude, altitude, tower, sensorType,measurement,measurement_value1,measurement_value2,measurement_value3,measurement_value4,measurement_value5)

    elif sensorType=='barometer':
        measurement='pressure'
        measurement_value1=reading[0]['value']
        return SolarPressureSensorData(time_stamp, latitude, longitude, altitude, tower, sensorType,measurement, measurement_value1)

    elif sensorType=='tempAndHumidity':
        measurement='temp-humidity'
        measurement_value1=reading[0]['value']
        measurement_value2=reading[1]['value']
        return TempHumiditySensorData(time_stamp, latitude, longitude, altitude, tower, sensorType, measurement, measurement_value1, measurement_value2)
    elif sensorType=='pyranometer':
        measurement='solarRadiance'
        measurement_value1=reading[0]['value']
        return SolarPressureSensorData(time_stamp, latitude, longitude, altitude, tower, sensorType, measurement, measurement_value1)

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
        print("Data uploaded to InfluxDB")
    if sensor_data.sensorType=='barometer':
        json_body=[
        {
            'measurement': sensor_data.measurement,
            'tags':{
                'latitude': sensor_data.latitude,
                'longitude': sensor_data.longitude,
                'Time_stamp': sensor_data.timestamp,
                'tower':sensor_data.Tower,
                'altitude':sensor_data.Altitude,
                'sensorType': sensor_data.sensorType

            },
            'fields': {
                'measurement_value1': sensor_data.measurement_value_1
            }
        }


        ]

        write_api.write(BUCKET, ORG, json_body)
        print("Data uploaded to InfluxDB")

    elif sensor_data.sensorType=='tempAndHumidity':
        json_body=[
        {
            'measurement': sensor_data.measurement,
            'tags':{
                'latitude': sensor_data.latitude,
                'longitude': sensor_data.longitude,
                'Time_stamp': sensor_data.timestamp,
                'tower':sensor_data.Tower,
                'altitude':sensor_data.Altitude,
                'sensorType': sensor_data.sensorType

            },
            'fields': {
                'measurement_value1': sensor_data.measurement_value_1,
                'measurement_value2': sensor_data.measurement_value_2


            }
        }


        ]
        write_api.write(BUCKET, ORG, json_body)
        print("Data uploaded to InfluxDB")



    elif sensor_data.sensorType == 'pyranometer':
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
                        'measurement_value1': sensor_data.measurement_value_1

                    }
                }

            ]
        write_api.write(BUCKET, ORG, json_body)
        print("Data uploaded to InfluxDB")



def on_message(client, userdata, message):
    print("Message received", str(message.payload.decode("utf-8")))
    data=str(message.payload.decode("utf-8"))
    print("Message topic", message.topic)
    sensor_data= parse_mqtt_message(message.topic,message.payload.decode("utf-8"))
    #print("Sensor Data Tuple", sensor_data)
    if sensor_data is not None:
        send_sensor_data_to_influx(sensor_data)
def on_connect(client,userdata,flags,rc):
    ## Callback for when the client receives a CONNACK response from server ##
    print("Connected with result code", rc)
    client.subscribe(MQTT_TOPIC)


def main():
    mqtt_client_ID=ID
    client=mqtt.Client(mqtt_client_ID)
    client.on_connect=on_connect
    client.on_message=on_message


    print("Connecting to broker")

    client.connect(host=HOST,port=MQTT_PORT)
    client.loop_forever()


    print("subscribing to topic")


print("**** MQTT subscribing ****")
main()
