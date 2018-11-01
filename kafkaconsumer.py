from kafka import KafkaConsumer, KafkaProducer
from pickle import loads, dumps
import time
import requests
from datetime import datetime


def cleanDocument(document):
    try:
        del document['current_vote']
    except KeyError:
        pass
    try:
        del document['sounds']
    except KeyError:
        pass

    return document


def addRatio(document):
    document = dict(document)
    up = document['thumbs_up']
    down = document['thumbs_down']
    if (up + down) == 0 or (up + down) < 50:
        ratio = 0
    else:
        ratio = up / (up + down)
    document['ratio'] = ratio
    return document


def fetchDocument(def_id=None):
    url = 'http://api.urbandictionary.com/v0/define'
    response = requests.get(url, params={'defid': str(def_id)})

    status_code = response.status_code
    if status_code == 429:
        print("Waiting for API...")
    t = 1
    # Status code 429 states that the API limit is reached
    while(status_code == 429):
        time.sleep(t)
        t *= 2
        response = requests.get(url, params={'defid': str(def_id)})
        status_code = response.status_code
    t = 1
    response_json = response.json()
    if 'list' in response_json and len(response_json['list']) != 0:
        fetched_document = response_json['list'][0]
    else:
        fetched_document = dict()
    return [fetched_document, status_code]


def addTimestamp(document, fetched_document):
    document = dict(document)
    try:
        date = datetime.strptime(fetched_document['written_on'],
                                 '%Y-%m-%dT%H:%M:%S.%fZ')
        document['written_on'] = date
    except KeyError:
        document['written_on'] = ""
    return document


def validateDocument(document, fetched_document):
    document = dict(document)
    try:
        if document['author'] != fetched_document['author']:
            document['author'] = fetched_document['author']
    except KeyError:
        pass

    try:
        if document['defid'] != fetched_document['defid']:
            document['defid'] = fetched_document['defid']
    except KeyError:
        pass

    try:
        if document['definition'] != fetched_document['definition']:
            document['definition'] = fetched_document['definition']
    except KeyError:
        pass

    try:
        if document['example'] != fetched_document['example']:
            document['example'] = fetched_document['example']
    except KeyError:
        pass

    try:
        if document['permalink'] != fetched_document['permalink']:
            document['permalink'] = fetched_document['permalink']
    except KeyError:
        pass

    try:
        if document['thumbs_up'] != fetched_document['thumbs_up']:
            document['thumbs_up'] = fetched_document['thumbs_up']
    except KeyError:
        pass

    try:
        if document['thumbs_down'] != fetched_document['thumbs_down']:
            document['thumbs_down'] = fetched_document['thumbs_down']
    except KeyError:
        pass

    try:
        if document['word'] != fetched_document['word']:
            document['word'] = fetched_document['word']
    except KeyError:
        pass

    try:
        document['lowercase_word'] = document['word'].lower()
    except KeyError:
        pass

    return document


def f(document):
    cleaned_document = cleanDocument(document)
    cleaned_document = addRatio(document)
    try:
        def_id = cleaned_document['defid']
    except KeyError:
        print("defid not found for {}".format(cleaned_document['_id']))
        return
    fetched_document, status_code = fetchDocument(def_id)
    if status_code == 200:
        added_timestamp = addTimestamp(cleaned_document, fetched_document)
        validated_document = validateDocument(
            added_timestamp, fetched_document)
    else:
        validated_document = dict(cleaned_document)
    validated_document['status_code'] = status_code
    return validated_document


# Connect to Kafka server on AWS as consumer
consumer = KafkaConsumer(
    'raw_documents',
    bootstrap_servers=['ec2-52-24-5-219.us-west-2.compute.amazonaws.com:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=10,
    group_id='my-group',
    value_deserializer=lambda x: loads(x))

# Connect to Kafka server on AWS as producer
producer = KafkaProducer(bootstrap_servers=['ec2-52-24-5-219.us-west-2.compute.amazonaws.com:9092'],
                         value_serializer=lambda x:
                         dumps(x))

# Publish enriched documents to Kafka
for message in consumer:
    message = message.value
    print('{} consumed'.format(message))
    validated_document = f(message)
    producer.send('enriched_documents', value=validated_document)
