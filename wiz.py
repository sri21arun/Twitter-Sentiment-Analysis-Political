import tweepy
from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from time import sleep
import json

consumer_key='bNz7NoNDs02g8tCQDUQmuiM02'
consumer_secret='M9iRDhRket61hy9tjymyZHxkWr66W8Yp6ywhCeK0lesARP7JH3'
access_token='953316372-biu6eiiSBRXjXQuvQDpWGT5ro99aWgL2ZPetouLa'
access_token_secret='iuAAhjXotRhPNTyrRB3JN8GbUM8D8H4BqorbaImflHAH7'

topic = b'twitter'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages(topic, json.dumps(data).encode('utf-8'))
        #print data
        return True

    def on_error(self, status):
        print status

WORDS_TO_TRACK = "#trump #obama".split()

if __name__ == '__main__':
    print 'running the twitter-stream python code'
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    # Goal is to keep this process always going
    while True:
        try:
           # stream.sample()
            stream.filter(languages=["en"], track=WORDS_TO_TRACK)
        except:
            pass
