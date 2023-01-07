import paho.mqtt.client as mqtt
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


MQTT_TOPIC = [(f"dvpg/msa/tower/1/height/1000/sonic", 0), (f"dvpg/msa/tower/2/height/1000/sonic", 0),
              (f"dvpg/msa/tower/4/height/1000/sonic", 0), (f"dvpg/msa/tower/6/height/1000/sonic", 0),
              (f"dvpg/msa/tower/7/height/1000/sonic", 0), (f"dvpg/msa/tower/8/height/1000/sonic", 0),
              (f"dvpg/msa/tower/9/height/1000/sonic", 0), (f"dvpg/msa/tower/10/height/1000/sonic", 0),
              (f"dvpg/msa/tower/11/height/1000/sonic", 0)]
'''




class mqttUtils:
    def __init__(self,broker,port):
        self.broker=broker
        self.port=port
        self.clientID='Subscriber'
        self.client=mqtt.Client()
        self.timeout_seconds=60
        self.client.on_connect=self.on_connect
        self.client.on_message=self.on_message

    def connect(self):
        self.client.connect(self.broker,self.port,self.timeout_seconds)

    def on_connect(client, topic, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        return str(rc)

    def mqttpublish(self,topic,data,qos,retain):
        self.client.publish(topic, data,qos=qos,retain=retain)

    def mqttsubscribe(self, topic):
        self.client.subscribe(topic)

    def on_message(client, userdata, msg):
            print("Inside message")
            dataReceived = msg.payload.decode("utf-8")
            print(msg.topic + " topic returns following data: " + str(msg.payload))

            return dataReceived

    def loop_start(self):
        self.client.loop_start()

    def loop_forever(self):
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            exit('│CTRL+C│ Exit by KeyboardInterrupt')

