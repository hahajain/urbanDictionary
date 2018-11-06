from time import sleep
from pickle import dumps
from kafka import KafkaProducer
from pymongo import MongoClient

# Connect to Kafka Server on AWS EC2 as a producer 
producer = KafkaProducer(bootstrap_servers=['ec2-52-24-5-219.us-west-2.compute.amazonaws.com:9092'],
                         value_serializer=lambda x:
                         dumps(x))

# Connect to database on AWS EC2
client = MongoClient('mongodb://ec2-52-24-5-219.us-west-2.compute.amazonaws.com:27017')
collection = client.UrbanDictionary.dictionary

# Publish data on a Kafka topic called raw_documents
for e in collection.find():
    producer.send('raw_documents', value=e)
    print("added {}".format(e))
