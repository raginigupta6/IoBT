# Kafka Utils Class with functions to access the Pub-sub functionality

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pickle


class KafkaUtils:
    def __init__(self, broker, port):
        self.broker = broker
        self.port = port
        self.bootstrap_server = self.broker + ':' + self.port
        self.client_id = 'ucla-pub-sub'
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_server, client_id = self.client_id)
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, max_request_size=104857600) #100 MB max data size

        self.consumer = None
        self.cosumer_topic = None

        self.consumer_timeout_ms = 5000


    def updateBroker(self,broker,port):
        self.broker = broker
        self.port = port
        self.bootstrap_server = self.broker + ':' + self.port
        self.client_id = 'ucla-pub-sub'
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_server, client_id = self.client_id)
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, max_request_size=1048576*100) #100 MB max data size

        self.consumer = None
        self.cosumer_topic = None


    def createTopic(self, topic_name):
        topic_to_create = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)

        if self.admin_client:
            returned = self.admin_client.create_topics(new_topics=[topic_to_create], validate_only=False)
            return returned

        return None

    def createTopicDistributed(self, topic_name, replication_factor=1):
        topic_to_create = NewTopic(name=topic_name, num_partitions=1, replication_factor=replication_factor)

        if self.admin_client:
            returned = self.admin_client.create_topics(new_topics=[topic_to_create], validate_only=False)
            return returned

        return None


    def sendData(self, topic_name, data):
        # Can send any python construct by serializing it using pickle
        payload = str.encode(str(pickle.dumps(data)))
        key = str.encode(topic_name)

        to_send = self.producer.send(topic_name, value=payload, key=key)
        self.producer.flush()
        record_metadata = to_send.get(timeout=10)
        return record_metadata


    def receiveData(self, topic_name):
        #user need to know the structure of data. This can be done by printing data.
        if self.cosumer_topic == None or self.cosumer_topic!=topic_name:
            self.cosumer_topic=topic_name
            self.consumer = KafkaConsumer(self.cosumer_topic, bootstrap_servers=self.bootstrap_server, consumer_timeout_ms = self.consumer_timeout_ms)


        for msg in self.consumer:
            val = msg.value.decode("utf-8")
            val = pickle.loads(eval(val))
            return val

        #in here, we have no data
        print("Request timeout")

    def seeTopics(self):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_server)
        #retrieve the available topics
        available_topics = consumer.topics()
        return available_topics

    def describeTopic(self, topic_name):

        if self.admin_client:
            configs = self.admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, topic_name)])
            return configs