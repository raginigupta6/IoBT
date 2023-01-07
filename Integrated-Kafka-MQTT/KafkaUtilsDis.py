# Kafka Utils Class with functions to access the Pub-sub functionality

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pickle


class KafkaUtils:
    ports = ['9092', '9093', '9094']
    idx = 0

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

    def __init__(self):
        done = False
        retry = 10
        while not done:
            try:
                self.broker = 'localhost'
                self.port = self.ports[self.idx]
                self.bootstrap_server = self.broker + ':' + self.port
                self.client_id = 'ucla-pub-sub'
                self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_server, client_id = self.client_id)
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, max_request_size=104857600) #100 MB max data size
                self.consumer = None
                self.cosumer_topic = None
                self.consumer_timeout_ms = 5000
                done = True
                print('found working kafka broker at:', self.ports[self.idx])

            except:
                print('Broker crashed at:', self.ports[self.idx], ' will look for a working broker')
                self.idx = (self.idx+1)%len(self.ports)
                retry -=1
                if retry<0:
                    done = True
                    print('No working broker found')

    def resetBroker(self):
        done = False

        retry = 10

        while not done:
            try:
                self.broker = 'localhost'
                self.port = self.ports[self.idx]
                self.bootstrap_server = self.broker + ':' + self.port
                self.client_id = 'ucla-pub-sub'
                self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_server, client_id = self.client_id)
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, max_request_size=104857600) #100 MB max data size
                self.consumer = None
                self.cosumer_topic = None
                self.consumer_timeout_ms = 5000
                done = True
                print('found working kafka broker at:', self.ports[self.idx])

            except:
                print('Broker crashed at:', self.ports[self.idx], ' will look for a working broker')
                self.idx = (self.idx+1)%len(self.ports)
                retry -=1
                if retry<0:
                    done = True
                    print('No working broker found')

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

        done = False
        retry = 10

        while not done:
            try:
                to_send = self.producer.send(topic_name, value=payload, key=key)
                self.producer.flush()
                record_metadata = to_send.get(timeout=10)
                done = True
            except:
                print('Current Broker failed')

                self.resetBroker()

                retry-=1
                if retry<0:
                    done = True

        return record_metadata


    def receiveData(self, topic_name):
        #user need to know the structure of data. This can be done by printing data.

        done = False
        retry = 10

        while not done:
            try:
                if self.cosumer_topic == None or self.cosumer_topic!=topic_name:
                    self.cosumer_topic=topic_name
                    self.consumer = KafkaConsumer(self.cosumer_topic, bootstrap_servers=self.bootstrap_server, consumer_timeout_ms = self.consumer_timeout_ms)

                done = True
                for msg in self.consumer:
                    val = msg.value.decode("utf-8")
                    val = pickle.loads(eval(val))
                    return val

            except:
                print('Current Broker failed')

                self.resetBroker()

                retry-=1
                if retry<0:
                    done = True

        return None

    def seeTopics(self):
        done = False
        retry = 10

        while not done:
            try:
                consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_server)
                available_topics = consumer.topics()
                done = True

            except:
                retry-=1
                self.resetBroker()

                if retry<0:
                    done = True

        return available_topics

    def describeTopic(self, topic_name):

        if self.admin_client:
            configs = self.admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, topic_name)])
            return configs
