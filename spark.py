import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import json
import nltk
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from geopy.geocoders import Nominatim

def retEmo(emotion):	
	emotions = sid.polarity_scores(emotion)
	emo = max(emotions, key=lambda high: emotions[high])
	values = { "neu" : 0, "neg" : -1,"pos" :1, "compound" : 0}
	return values[emo]

def get_locations(locations):
	geolocator = Nominatim()
	location = geolocator.geocode(locations)
	if location != None:
		return str(location.latitude) +"," + str(location.longitude)
	else:
		return "12.9791198,77.5912997"

if __name__ == "__main__":
    sc = SparkContext()
    sc.setLogLevel("WARN")
    stream=StreamingContext(sc,2)
    topic = sys.argv[1]
    brokers = sys.argv[2]
    sid = SentimentIntensityAnalyzer()
    kafkaStream = KafkaUtils.createDirectStream(stream,[topic], {"metadata.broker.list": brokers}) 
    #json_ld = kafkaStream.map(lambda line:json.loads(line[1]))
    received = kafkaStream.map(lambda js:json.loads(js[1]))
    #json_ld.saveAsTextFiles("wizard21")
    #print'tweet',received.pprint() 
    #received.count().map(lambda c:"Number of tweets : " + str(c)).pprint();
    rec3 = received.map(lambda l:{"timestamp": json.loads(l)["timestamp_ms"],"emotions":json.dumps(retEmo(json.loads(l)["text"])),\
   		"geo": get_locations(json.loads(l)["coordinates"]),"text":json.loads(l)["text"]})
    #rec3.pprint()
    rec4 = rec3.map(lambda l:(None,json.dumps(l)))
    rec4.pprint()
    conf = {"es.resource": "wizard/analysis", "es.input.json": "true"}
    rec4.foreachRDD(lambda line:line.saveAsNewAPIHadoopFile(
    	path="-",
    	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    	keyClass="org.apache.hadoop.io.NullWritable",
    	valueClass= "org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    	conf=conf))

    stream.start()
    stream.awaitTermination()
