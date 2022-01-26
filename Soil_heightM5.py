import time
import pandas as pd
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import NamedTuple
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


## For Sonic Sensor Data at height 200
'''

Topics information
dvpg/msa/tower/1/height/200/pressure

dvpg/msa/tower/1/height/200/tempandhumidity

dvpg/msa/tower/1/height/200/solarradiance

dvpg/msa/tower/1/height/200/sonic

dvpg/msa/tower/1/height/1000/sonic

dvpg/msa/tower/1/height/1000/temperature

dvpg/msa/tower/1/height/-10/soil

dvpg/msa/tower/1/height/-5/soil

dvpg/msa/tower/1/surface/precipitations

dvpg/msa/tower/1/enclosure/cr6

dvpg/msa/tower/1/surface/soil


'''

import paho.mqtt.client as mqtt
TOKEN = "Fdh-kOfYFmROa9moZQeprDTKfP20WRIk8ZKbrYeyTVkc_DViQyOm0fRW-hBoY6nN3Q83D7BlCiW30AlYivL6KA=="
ORG = "UIUC"
BUCKET = "MSAdata"
HOST="localhost"
MQTT_PORT=1883
#INFLUXDB_CLIENT = InfluxDBClient(url="http://localhost:8086", token=TOKEN)
#write_api = INFLUXDB_CLIENT.write_api(write_options=SYNCHRONOUS) ## 1,2,4,6,7,8,9,10,11
MQTT_TOPIC = [(f"dvpg/msa/tower/1/height/-5/soil", 0), (f"dvpg/msa/tower/2/height/-5/soil", 0),
            (f"dvpg/msa/tower/3/height/-5/soil", 0), (f"dvpg/msa/tower/4/height/-5/soil", 0),
              (f"dvpg/msa/tower/5/height/-5/soil", 0), (f"dvpg/msa/tower/6/height/-5/soil", 0),
              (f"dvpg/msa/tower/7/height/-5/soil", 0), (f"dvpg/msa/tower/8/height/-5/soil", 0),
              (f"dvpg/msa/tower/9/height/-5/soil", 0), (f"dvpg/msa/tower/10/height/-5/soil", 0),
              (f"dvpg/msa/tower/11/height/-5/soil", 0), (f"dvpg/msa/tower/12/height/-5/soil", 0)]

class SoilM5SensorData(NamedTuple):
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
        measurement='25JanSonic200' ## Corresponds to Table Name
        measurement_value1=reading[0]['value']
        measurement_value2=reading[1]['value']
        measurement_value3=reading[2]['value']
        measurement_value4=reading[3]['value']
        measurement_value5=reading[4]['value']
        return SoilM5SensorData(time_stamp, latitude, longitude, altitude, tower, sensorType,measurement,measurement_value1,measurement_value2,measurement_value3,measurement_value4,measurement_value5)


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
        #write_api.write(BUCKET, ORG, json_body)
        #print("Data uploaded to InfluxDB")

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    ## For towers: 1,2,4,6,7,8,9,10,11

    ##
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    #print("inside message")

    print(msg.topic + " " + str(msg.payload))
    line=msg.payload.decode("utf-8")+"\n"
    outfile.write(line)
    print(msg.payload.decode("utf-8"))
    #sensor_data= parse_mqtt_message(msg.topic,msg.payload.decode("utf-8"))
    #print("Sensor Data Tuple", sensor_data)
    #if sensor_data is not None:
    #    send_sensor_data_to_influx(sensor_data)

outfile=open('./soilM5Data.txt','w')
client = mqtt.Client()

client.connect("192.168.250.51", 1883, 60)
client.on_connect = on_connect

client.on_message = on_message


client.loop_forever()
time.sleep(0.1)