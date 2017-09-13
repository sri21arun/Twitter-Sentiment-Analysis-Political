Q4. 
#Start Zookeeper
zkserver

#Start Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

#Create a Topic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter

#Start Producer
kafka-console-producer.bat --broker-list localhost:9092 --topic twitter

#Start Consumer
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic twitter

#Run Producer
spark-submit wiz.py

#Start elasticsearch
elasticsearch.bat

#Run Consumer
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar,elasticsearch-hadoop-5.3.0.jar spark.py twitter localhost:9092

#Start Kibana
kibana.bat
