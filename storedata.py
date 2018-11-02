from kafka import KafkaConsumer
from pickle import loads
from pymongo import MongoClient

consumer = KafkaConsumer(
    'enriched_documents',
    bootstrap_servers=['ec2-52-24-5-219.us-west-2.compute.amazonaws.com:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=0,
    group_id='my-group',
    value_deserializer=lambda x: loads(x))

client = MongoClient('mongodb://ec2-52-24-5-219.us-west-2.compute.amazonaws.com:27017')
collection = client.UrbanDictionary.updated_dictionary

for message in consumer:
    message = message.value
    print('{} pushed'.format(message))
    collection.insert_one(message)
