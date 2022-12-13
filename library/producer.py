from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['172.18.0.1:9092'])
key = "Department"
value = "Something"

while True:
    producer.send('my-topic', json.dumps(value, ensure_ascii=False).encode('utf8') )
    producer.flush()
