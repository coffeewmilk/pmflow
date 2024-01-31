from kafka import KafkaProducer
import time
import requests
import pandas as pd
import json

# environment variable
aqicnKey = '[aqicnKey]'
latlong1 = ['13.898819', '100.415717']
latlong2 = ['13.579757', '100.683936']

# initiate 
producer = KafkaProducer(bootstrap_servers=['192.168.100.239:9092'], value_serializer=lambda m: m.encode())


def PMunit():

    PMStations = requests.get(f"https://api.waqi.info/v2/map/bounds?latlng={','.join(latlong1)},{','.join(latlong2)}&token={aqicnKey}").text
    # is it better as list or pandas dataframe to extract this json?
    # if the api failed to fetch data how to we raise error?
    # PMDataFrame = pd.json_normalize(PMStations, record_path='data')
    # print(PMStations)
    return (PMStations)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback ', exc_info=excp)


while True:
    
    producer.send('topic', PMunit()).add_callback(on_send_success).add_errback(on_send_error)
    time.sleep(0.1)



