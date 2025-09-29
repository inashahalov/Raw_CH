# scripts/kafka_producer.py

from __future__ import annotations
import json
import time
from kafka import KafkaProducer
from pymongo import MongoClient
from cryptography.fernet import Fernet

# === Генерация ключа шифрования ===
ENCRYPTION_KEY = Fernet.generate_key()
cipher = Fernet(ENCRYPTION_KEY)

print(f"🔑 Ключ шифрования (сохраните!): {ENCRYPTION_KEY.decode()}")

# === Шифрование ===
def encrypt_field(value: str | None) -> str:
    if not value:
        return ""
    return cipher.encrypt(value.encode()).decode()

# === Нормализация ===
def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return f"+{digits}"
    return phone  # fallback

def normalize_email(email: str | None) -> str:
    return email.strip().lower() if email else ""

# === Подключение к Kafka и MongoDB ===
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
        doc.pop('_id', None)
        doc['_collection'] = coll_name

        # === Шифруем email и phone ===
        if coll_name == 'customers':
            email = doc.get('email')
            phone = doc.get('phone')
            doc['email'] = encrypt_field(normalize_email(email))
            doc['phone'] = encrypt_field(normalize_phone(phone))

        if coll_name == 'stores':
            email = doc['manager'].get('email')
            phone = doc['manager'].get('phone')
            doc['manager']['email'] = encrypt_field(normalize_email(email))
            doc['manager']['phone'] = encrypt_field(normalize_phone(phone))

        producer.send('piccha_raw', value=doc)
        print(f"Отправлено в Kafka: {coll_name} - {doc.get('store_id') or doc.get('id') or doc.get('customer_id') or doc.get('purchase_id')}")

        time.sleep(0.01)

producer.flush()
print("✅ Все данные отправлены в Kafka топик 'piccha_raw'")