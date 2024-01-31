from kafka import KafkaConsumer
import time
import requests
import pandas as pd
import json

consumer = KafkaConsumer('topic', bootstrap_servers=['localhost:9092'], value_deserializer=lambda m: m.decode())

while True:
    print(consumer.poll(timeout_ms = 1000))

