"""

Benchmarking Kafka:
- Message Delays of sending messages
- We will simulate sender and receiver as a separate threads

"""

from KafkaUtils import *
import time
import threading
from multiprocessing import Process
import multiprocessing

broker = "127.0.0.1"
port = "9092"

pubsub = KafkaUtils(broker = broker, port = port)
global_delays1 = []
throughput = 0

throughputs = []

# A subscriber to send the data
def sender1(topic_name = 'sensor-2', repeat = 100):
    print('Starting Sender:', topic_name)

    for i in range(repeat):
        data_to_send = [i,  time.time()]
        pubsub.sendData(topic_name= topic_name, data = data_to_send)
        #print('I is:', i)
    print('Sender completed-1:',topic_name)


def sender2(topic_name = 'sensor-2', repeat = 100):
    #print('Starting Sender-2:', topic_name, repeat)

    broker = "127.0.0.1"
    port = "9092"

    pubsub = KafkaUtils(broker=broker, port=port)

    for i in range(repeat):
        data_to_send = [time.time(), [1]*180] #[i,  time.time()]
        #print('Trying to send data')
        pubsub.sendData(topic_name= topic_name, data = data_to_send)
        #print('Sender-2 is:', i)

    #print('Sender completed-2:',topic_name)

def receiver1(topic_name = 'sensor-2', repeat = 100):
    global global_delays1

    print('Starting receiver-1:',topic_name, repeat)
    for i in range(repeat):
        data = pubsub.receiveData(topic_name= topic_name)
        receive_time = time.time()

        if data!=None:
            #print('Data:',data)
            delay = (receive_time-float(data[1]))*1000.0
            global_delays1.append(delay)
            #print('Message Delay:', delay)

    print('Done receiver-1:',topic_name)


def receiver2(topic_name = 'sensor-2', repeat = 100, return_dict=None):

    #print('Starting receiver-2:', topic_name, repeat)

    data = pubsub.receiveData(topic_name= topic_name)
    data = pubsub.receiveData(topic_name= topic_name)

    st_time = time.time()

    for i in range(repeat-2):
        data = pubsub.receiveData(topic_name= topic_name)
        #receive_time = time.time()
        #print(topic_name, ':', i)

    end_time = time.time()

    throughput = (repeat-2)/((end_time-st_time))
    return_dict[topic_name] = throughput

    #print('throughput:', throughput, st_time, end_time)
    #print('Done receiver-2:',topic_name)

import pickle

def save_data(data, file_name):
    with open(file_name, 'wb') as handle:
        pickle.dump(data, handle)

#more number of publishers-subscribers
def Scenario3(num_of_pub_sub = 10):
    receivers = []
    senders = []

    for i in range(num_of_pub_sub):
        r = threading.Thread(target=receiver1, args=('sensor-'+str(i+1), 100))
        receivers.append(r)

        s = threading.Thread(target=sender1, args=('sensor-'+str(i+1), 1000))
        senders.append(s)

    for r in receivers:
        r.start()

    time.sleep(2)

    for s in senders:
        s.start()

    for s in senders:
        s.join()

    # for r in receivers:
    #     r.join()


#more number of publishers-subscribers
def Scenario4(num_of_pub_sub = 10):
    receivers = []
    senders = []

    manager = multiprocessing.Manager()
    return_dict = manager.dict()


    for i in range(num_of_pub_sub):
        r = Process(target=receiver2, args=('sensor-'+str(i+1), 100, return_dict))
        receivers.append(r)

        s = Process(target=sender2, args=('sensor-'+str(i+1), 1000))
        senders.append(s)

    for r in receivers:
        r.start()

    time.sleep(3)

    for s in senders:
        s.start()

    for s in senders:
        s.join()

    for r in receivers:
        r.join()

    return return_dict

def createTopic(num_of_topics):
    for i in range(num_of_topics):
        topic_name = 'sensor-'+str(i+1)
        try:
            pubsub.createTopic(topic_name)
        except:
            pass

if __name__ == "__main__":

    createTopic(100)

    #Scenario1()
    #Scenario2()

    #Scenario3()

    num_of_topics_list = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    for n in num_of_topics_list:
        return_dict = Scenario4(n)
        throughputs = return_dict.values()
        #print(return_dict.values())
        #save_data(global_delays1, 'kafka_delays_10.pickle')
        #print(global_delays1)
        print(n, 'throughputs:', sum(throughputs))

    print("*"*50)
    print('All done')
