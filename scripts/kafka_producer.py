# scripts/kafka_producer.py
import json
import time

from kafka import KafkaProducer
from pymongo import MongoClient

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

client = MongoClient('mongodb://localhost:27018/')
db = client['piccha_db']

collections = ['stores', 'products', 'customers', 'purchases']

for coll_name in collections:
    collection = db[coll_name]
    for doc in collection.find():
        doc.pop('_id', None)  # Удаляем ObjectId, который не сериализуется в JSON
        doc['_collection'] = coll_name
        producer.send('piccha_raw', value=doc)
        print(f"Отправлено в Kafka: {coll_name} - {doc.get('store_id') or doc.get('id') or doc.get('customer_id') or doc.get('purchase_id')}")
        time.sleep(0.01)

producer.flush()
print("✅ Все данные отправлены в Kafka топик 'piccha_raw'")