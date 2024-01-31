from kafka import KafkaConsumer
import time
import requests
import pandas as pd
import json

consumer = KafkaConsumer('myTopic', bootstrap_servers=['localhost:29092'], value_deserializer=lambda m: m.decode())

while True:
    print(consumer.poll(timeout_ms = 1000))

