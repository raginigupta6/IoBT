# Tools for installation
1. MOSQUITTO MQTT Broker (Command: sudo apt install mosquitto)
2. InfluxDB (https://docs.influxdata.com/influxdb/v2.0/install/?t=Linux)
3. Grafana (https://grafana.com/docs/grafana/latest/installation/debian/)

# Required Python Libraries:
1. Pandas
2. paho-mqtt (pip install paho-mqtt)
3. Influxdb-client (pip install influxdb-client)
4. Scikit-learn
5. Numpy


## File descriptions:
1. The Publisher.py file reads the sensor data from SensorData.txt file and publishes it to the mosquitto MQTT broker
2. The Subsciber.py file subscribes to the topic of interest and reads the data from MQTT broker
3. Subscriber.py parses the data, intiates influxDB instance and writes live sensor data to the InfluxDB database
4. The AnomalyDetection.py file executes two anomaly detection algorithms; Centroid-based anomaly detection and Isolation Forest algorithm on the pre-processed sensor data file, AnomalyData.xlsx


## Steps:
Note that all tools and python libraries are already installed in the Virtual Machine provided. 

1. Download virtual box for the VM image (https://www.virtualbox.org/wiki/Downloads)
2. Download VM .ova file from https://uofi.box.com/s/ate29b0qb7jfutewp0h2mvi3s3037166
3. Import VM .ova file in Virtual box\
   **VM password: virtualmachine1**
4. Open Pycharm and head to the project >> "mqtt_anomalyDetection"
5. Open  terminal window and type “mosquitto” to start MQTT mosquito broker\
   If there is an error with busy broker address, enter the following command on terminal to get the process ID:\
   &gt; &gt; ps -ef | grep mosquitto\
   Kill the process with the following command:\
   &gt; &gt;sudo kill "process ID"
6. Once the mosquito broker is up and running, execute the subscriber code from Pycharm Project
7. Execute the publisher code (from terminal window). Ensure that both Publisher.py and Subscriber.py run as two independent processes.\
   &gt; &gt;python3 /home/virtualmachine1/PycharmProjects/mqtt_anomalyDetection/Publisher.py
8. To start the InfluxDB instance, enter the following command on terminal window:\
   &gt; &gt;cd /usr/local/bin\
   &gt; &gt;influxd
9. Open http://localhost:8086 on browser to access InfluxDB. Log in details: Username= admin, Password= admin1234
10. Create a new bucket from Explorer tab at http://localhost:8086, if needed, to insert incoming sensor data into a new database from Subscriber
11. (Optional): Start Grafana instance using the command:\
   &gt; &gt;sudo /bin/systemctl start grafana-server\
   Open http://localhost:3000 on browser to access Grafana dashboard.
12. Initialize InfluxDB data source on Grafana Dashboard by providing InfluxDB's token, bucket and org credentials. Build dashboard and execute Flux queries on Grafana Editor.
13. Run the Anomaly Detection algorithm from AnomalyDetection.py and report anomalies with their corresponding anomaly score and timestamp.


Details of this work can be found at: 

1. https://dl.acm.org/doi/abs/10.1145/3524053.3542742

2. https://ieeexplore.ieee.org/abstract/document/9594944/
