import paho.mqtt.client as mqtt
import KafkaUtils
import time

mqtt_broker='192.168.250.51' # or 192.168.250.51
mqtt_client=mqtt.Client('MQTTBridge')
mqtt_client.connect(mqtt_broker)
MQTT_TOPIC = [(f"dvpg/msa/tower/1/height/1000/sonic", 0), (f"dvpg/msa/tower/2/height/1000/sonic", 0),
              (f"dvpg/msa/tower/4/height/1000/sonic", 0), (f"dvpg/msa/tower/6/height/1000/sonic", 0),
              (f"dvpg/msa/tower/7/height/1000/sonic", 0), (f"dvpg/msa/tower/8/height/1000/sonic", 0),
              (f"dvpg/msa/tower/9/height/1000/sonic", 0), (f"dvpg/msa/tower/10/height/1000/sonic", 0),
              (f"dvpg/msa/tower/11/height/1000/sonic", 0)]

# Insert Logic To Check which Kafka Broker is up and then send the data to it
kafka_broker="localhost" # IP address
kafka_port="29092"
kafkaInstance = KafkaUtils(broker = kafka_broker, port = kafka_port)
kafka_topic=""  ## Where Kafka wants to publish
kafkaInstance.createTopic(kafka_topic)

def on_message(client,userdata,message):

    msg_payload=str(message.payload)
    print("Received MQTT message", msg_payload)
    data_to_send=message.payload.decode("utf-8")


    kafkaInstance.sendData(topic_name=kafka_topic, data=msg_payload)
    print("Kafka Published:  ", data_to_send)



mqtt_client.subscribe(MQTT_TOPIC)
mqtt_client.on_message=on_message
mqtt_client.loop_forever()