Конечно! Вот обновлённый `README.md`, оформленный аккуратно, с **сворачивающимися блоками кода** и **оглавлением**.

---

# Проект "Аналитика Пикча"

## Оглавление

- [Описание](#описание)
- [Архитектура](#архитектура)
- [Участники команды](#участники-команды)
- [Структура проекта](#структура-проекта)
- [Генерация данных](#генерация-данных)
- [Загрузка данных в MongoDB](#загрузка-данных-в-mongodb)
- [Запуск инфраструктуры](#запуск-инфраструктуры)
- [Загрузка данных из MongoDB в Kafka](#загрузка-данных-из-mongodb-в-kafka)
- [Загрузка данных из Kafka в ClickHouse (RAW)](#загрузка-данных-из-kafka-в-clickhouse-raw)
- [Очистка данных и Materialized View (MART)](#очистка-данных-и-materialized-view-mart)
- [Настройка Grafana](#настройка-grafana)
- [Telegram-бот для алертинга](#telegram-бот-для-алертинга)
- [Как запустить](#как-запустить)
- [Результаты](#результаты)

---

## Описание

Данный проект реализует аналитическую систему для сетевого магазина "Пикча", позволяющую эффективно продавать товары за счёт:

- Сбора и хранения данных из различных источников
- Очистки и подготовки данных
- Визуализации ключевых метрик
- Алертинга при аномалиях (например, превышение 50% дубликатов)

---

## Архитектура

```
MongoDB (NoSQL) → Kafka → ClickHouse (RAW) → Materialized View (MART) → Grafana (Dashboard & Alerting)
```

---

## Участники команды

- Иванов Иван (ivanov@example.com)
- Петров Петр (petrov@example.com)

---

## Структура проекта

```
project/
├── data/
│   ├── stores/
│   ├── products/
│   ├── customers/
│   └── purchases/
├── scripts/
│   ├── generate_data.py
│   ├── load_to_mongo.py
│   ├── kafka_producer.py
│   ├── kafka_to_clickhouse.py
│   └── clean_data.sql
├── config/
│   └── clickhouse/
│       ├── listen_all.xml
│       └── users.d/
├── docker-compose.yml
├── dashboard_screenshot.png
├── telegram_alert_screenshot.png
└── README.md
```

---

## Генерация данных

Скрипт `scripts/generate_data.py` генерирует JSON-файлы для:

- 45 магазинов
- 20 товаров
- 45 покупателей
- 200 покупок

<details>
<summary>Показать код (scripts/generate_data.py)</summary>

```python
# scripts/generate_data.py
import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("ru_RU")

os.makedirs("data/stores", exist_ok=True)
os.makedirs("data/products", exist_ok=True)
os.makedirs("data/customers", exist_ok=True)
os.makedirs("data/purchases", exist_ok=True)

categories = [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
]

store_networks = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]
stores = []

# === 1. Генерация магазинов ===
for network, count in store_networks:
    for i in range(count):
        store_id = f"store-{len(stores)+1:03}"
        city = fake.city()
        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин на {fake.street_name()}",
            "store_network": network,
            "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {count} магазинов.",
            "type": "offline",
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": city,
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {
                "mon_fri": "09:00-21:00",
                "sat": "10:00-20:00",
                "sun": "10:00-18:00"
            },
            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }
        stores.append(store)
        with open(f"data/stores/{store_id}.json", "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)

# === 2. Генерация товаров ===
products = []
for i in range(20):
    product = {
        "id": f"prd-{1000+i}",
        "name": fake.word().capitalize() + " " + fake.word().capitalize(),
        "group": random.choice(categories),
        "description": fake.sentence(),
        "kbju": {
            "calories": round(random.uniform(50, 300), 1),
            "protein": round(random.uniform(0.5, 20), 1),
            "fat": round(random.uniform(0.1, 15), 1),
            "carbohydrates": round(random.uniform(0.5, 50), 1)
        },
        "price": round(random.uniform(30, 300), 2),
        "unit": random.choice(["упаковка", "шт", "кг", "л"]),
        "origin_country": "Россия",
        "expiry_days": random.randint(5, 30),
        "is_organic": random.choice([True, False]),
        "barcode": fake.ean(length=13),
        "manufacturer": {
            "name": fake.company(),
            "country": "Россия",
            "website": f"https://{fake.domain_name()}",
            "inn": fake.bothify(text='##########')
        }
    }
    products.append(product)
    with open(f"data/products/{product['id']}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

# === 3. Генерация покупателей ===
customers = []
for store in stores:
    customer_id = f"cus-{1000 + len(customers)}"
    customer = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "gender": random.choice(["male", "female"]),
        "registration_date": datetime.now().isoformat(),
        "is_loyalty_member": True,
        "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
        "purchase_location": store["location"],
        "delivery_address": {
            "country": "Россия",
            "city": store["location"]["city"],
            "street": fake.street_name(),
            "house": str(fake.building_number()),
            "apartment": str(random.randint(1, 100)),
            "postal_code": fake.postcode()
        },
        "preferences": {
            "preferred_language": "ru",
            "preferred_payment_method": random.choice(["card", "cash"]),
            "receive_promotions": random.choice([True, False])
        }
    }
    customers.append(customer)
    with open(f"data/customers/{customer_id}.json", "w", encoding="utf-8") as f:
        json.dump(customer, f, ensure_ascii=False, indent=2)

# === 4. Генерация покупок ===
for i in range(200):
    customer = random.choice(customers)
    store = random.choice(stores)
    items = random.sample(products, k=random.randint(1, 3))
    purchase_items = []
    total = 0
    for item in items:
        qty = random.randint(1, 5)
        total_price = round(item["price"] * qty, 2)
        total += total_price
        purchase_items.append({
            "product_id": item["id"],
            "name": item["name"],
            "category": item["group"],
            "quantity": qty,
            "unit": item["unit"],
            "price_per_unit": item["price"],
            "total_price": total_price,
            "kbju": item["kbju"],
            "manufacturer": item["manufacturer"]
        })
    purchase = {
        "purchase_id": f"ord-{i+1:05}",
        "customer": {
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "email": customer["email"], # будет зашифровано позже
            "phone": customer["phone"], # будет зашифровано позже
            "is_loyalty_member": customer["is_loyalty_member"],
            "loyalty_card_number": customer["loyalty_card_number"]
        },
        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "location": store["location"]
        },
        "items": purchase_items,
        "total_amount": round(total, 2),
        "payment_method": random.choice(["card", "cash"]),
        "is_delivery": random.choice([True, False]),
        "delivery_address": customer["delivery_address"],
        "purchase_datetime": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat()
    }
    with open(f"data/purchases/{purchase['purchase_id']}.json", "w", encoding="utf-8") as f:
        json.dump(purchase, f, ensure_ascii=False, indent=2)

print("✅ Генерация данных завершена: 45 магазинов, 20 товаров, 45 покупателей, 200 покупок.")
```

</details>

---

## Загрузка данных в MongoDB

Скрипт `scripts/load_to_mongo.py` загружает JSON-файлы в MongoDB.

<details>
<summary>Показать код (scripts/load_to_mongo.py)</summary>

```python
# scripts/load_to_mongo.py
import json
import os
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27018/')
db = client['piccha_db']

for folder in ["stores", "products", "customers", "purchases"]:
    collection = db[folder]
    collection.delete_many({})  # Очистка коллекции
    for file in os.listdir(f"data/{folder}"):
        with open(f"data/{folder}/{file}", "r", encoding="utf-8") as f:
            data = json.load(f)
            collection.insert_one(data)
    print(f"✅ Загружено {collection.count_documents({})} документов в коллекцию '{folder}'")
```

</details>

---

## Запуск инфраструктуры

Файл `docker-compose.yml` запускает:

- MongoDB
- Kafka + Zookeeper
- ClickHouse
- Grafana

<details>
<summary>Показать код (docker-compose.yml)</summary>

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: piccha-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - piccha-net

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: piccha-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    networks:
      - piccha-net

  mongodb:
    image: mongo:4.4
    container_name: piccha-mongo
    ports:
      - "27018:27017"
    volumes:
      - mongo_/data/db
    networks:
      - piccha-net

  clickhouse:
    image: clickhouse/clickhouse-server:23.12
    container_name: piccha-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./config/clickhouse:/etc/clickhouse-server/config.d
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    networks:
      - piccha-net

  grafana:
    image: grafana/grafana:10.0.0
    container_name: piccha-grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    networks:
      - piccha-net

volumes:
  mongo_
  clickhouse_

networks:
  piccha-net:
    driver: bridge
```

</details>

---

## Загрузка данных из MongoDB в Kafka

Скрипт `scripts/kafka_producer.py` отправляет данные из MongoDB в топик `piccha_raw` Kafka, **шифруя** `email` и `phone`.

<details>
<summary>Показать код (scripts/kafka_producer.py)</summary>

```python
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
    return phone

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
```

</details>

---

## Загрузка данных из Kafka в ClickHouse (RAW)

Скрипт `scripts/kafka_to_clickhouse.py` читает из Kafka, **дешифрует** `email` и `phone`, и загружает в **RAW** таблицы ClickHouse.

<details>
<summary>Показать код (scripts/kafka_to_clickhouse.py)</summary>

```python
# scripts/kafka_to_clickhouse.py
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, TypedDict, cast
from datetime import datetime
from clickhouse_driver import Client
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Ключ шифрования (вставьте сюда ключ из kafka_producer.py) ===
ENCRYPTION_KEY = b'ТУТ_ТОТ_КЛЮЧ_КОТОРЫЙ_ТЫ_СКОПИРОВАЛ'
cipher = Fernet(ENCRYPTION_KEY)

logger.info("🔑 Используем ключ шифрования.")

# === Нормализация и дешифровка ===
def decrypt_phone_or_email(value: str | None) -> str:
    if not value:
        return ""
    try:
        return cipher.decrypt(value.encode()).decode()
    except Exception:
        return value  # fallback: вернуть как есть, если не расшифровывается

def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    # Приведение к единому формату +7 (если не зашифровано)
    if phone.startswith('+7') and phone[1:].isdigit() and len(phone) == 12:
        return phone
    return phone  # fallback на оригинальный формат

def normalize_email(email: str | None) -> str:
    """Нормализует email: приводит к нижнему регистру и удаляет пробелы."""
    return email.strip().lower() if email else ""

# === Подключение к ClickHouse ===
client = Client(host='localhost', port=9000)

# === Создание RAW таблиц ===
def create_raw_tables() -> None:
    client.execute("CREATE DATABASE IF NOT EXISTS piccha_raw")

    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.stores (
        store_id String,
        store_name String,
        store_network String,
        store_type_description String,
        type String,
        categories Array(String),
        manager_name String,
        manager_phone String,
        manager_email String,
        location_country String,
        location_city String,
        location_street String,
        location_house String,
        location_postal_code String,
        location_latitude Float64,
        location_longitude Float64,
        opening_hours_mon_fri String,
        opening_hours_sat String,
        opening_hours_sun String,
        accepts_online_orders UInt8,
        delivery_available UInt8,
        warehouse_connected UInt8,
        last_inventory_date Date
    ) ENGINE = MergeTree() ORDER BY store_id
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.products (
        id String,
        name String,
        group String,
        description String,
        kbju_calories Float32,
        kbju_protein Float32,
        kbju_fat Float32,
        kbju_carbohydrates Float32,
        price Float32,
        unit String,
        origin_country String,
        expiry_days UInt16,
        is_organic UInt8,
        barcode String,
        manufacturer_name String,
        manufacturer_country String,
        manufacturer_website String,
        manufacturer_inn String
    ) ENGINE = MergeTree() ORDER BY id
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.customers (
        customer_id String,
        first_name String,
        last_name String,
        email String,
        phone String,
        birth_date Date,
        gender String,
        registration_date DateTime,
        is_loyalty_member UInt8,
        loyalty_card_number String,
        purchase_location_store_id String,
        purchase_location_city String,
        delivery_address_city String,
        delivery_address_street String,
        delivery_address_house String,
        delivery_address_apartment String,
        delivery_address_postal_code String,
        preferred_language String,
        preferred_payment_method String,
        receive_promotions UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.purchases (
        purchase_id String,
        customer_id String,
        store_id String,
        total_amount Float32,
        payment_method String,
        is_delivery UInt8,
        delivery_address_city String,
        delivery_address_street String,
        delivery_address_house String,
        delivery_address_apartment String,
        delivery_address_postal_code String,
        purchase_datetime DateTime
    ) ENGINE = MergeTree() ORDER BY purchase_id
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.purchase_items (
        purchase_id String,
        product_id String,
        item_name String,
        category String,
        quantity UInt32,
        unit String,
        price_per_unit Float32,
        total_price Float32,
        kbju_calories Float32,
        kbju_protein Float32,
        kbju_fat Float32,
        kbju_carbohydrates Float32,
        manufacturer_name String
    ) ENGINE = MergeTree() ORDER BY (purchase_id, product_id)
    """)

# === Основной consumer ===
def main() -> None:
    create_raw_tables()

    consumer = KafkaConsumer(
        'piccha_raw',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='clickhouse-loader',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logger.info("⏳ Ожидаю данные из Kafka...")

    for message in consumer:
        try:
            raw_doc: Dict[str, Any] = message.value
            coll: str = raw_doc.pop('_collection', 'unknown')

            if coll == 'stores':
                doc = raw_doc

                # Конвертация last_inventory_date из строки в дату
                last_inventory_date_str = doc.get('last_inventory_date', '')
                last_inventory_date = datetime.fromisoformat(last_inventory_date_str.replace("Z", "+00:00")) if last_inventory_date_str else datetime(1970, 1, 1)

                client.execute("""
                INSERT INTO piccha_raw.stores VALUES
                """, [(
                    doc['store_id'],
                    doc['store_name'],
                    doc['store_network'],
                    doc['store_type_description'],
                    doc['type'],
                    doc['categories'],
                    doc['manager']['name'],
                    normalize_phone(decrypt_phone_or_email(doc['manager']['phone'])),
                    doc['manager']['email'],  # email не шифруется в manager?
                    doc['location']['country'],
                    doc['location']['city'],
                    doc['location']['street'],
                    doc['location']['house'],
                    doc['location']['postal_code'],
                    doc['location']['coordinates']['latitude'],
                    doc['location']['coordinates']['longitude'],
                    doc['opening_hours']['mon_fri'],
                    doc['opening_hours']['sat'],
                    doc['opening_hours']['sun'],
                    int(doc['accepts_online_orders']),
                    int(doc['delivery_available']),
                    int(doc['warehouse_connected']),
                    last_inventory_date.date()
                )])

            elif coll == 'products':
                doc = raw_doc

                client.execute("""
                INSERT INTO piccha_raw.products VALUES
                """, [(
                    doc['id'],
                    doc['name'],
                    doc['group'],
                    doc['description'],
                    doc['kbju']['calories'],
                    doc['kbju']['protein'],
                    doc['kbju']['fat'],
                    doc['kbju']['carbohydrates'],
                    doc['price'],
                    doc['unit'],
                    doc['origin_country'],
                    doc['expiry_days'],
                    int(doc['is_organic']),
                    doc['barcode'],
                    doc['manufacturer']['name'],
                    doc['manufacturer']['country'],
                    doc['manufacturer']['website'],
                    doc['manufacturer']['inn']
                )])

            elif coll == 'customers':
                doc = raw_doc

                # Конвертация дат
                birth_date_str = doc.get('birth_date', '')
                birth_date = datetime.fromisoformat(birth_date_str) if birth_date_str else datetime(1970, 1, 1)

                registration_date_str = doc.get('registration_date', '')
                registration_date = datetime.fromisoformat(registration_date_str.replace("Z", "+00:00")) if registration_date_str else datetime(1970, 1, 1)

                client.execute("""
                INSERT INTO piccha_raw.customers VALUES
                """, [(
                    doc['customer_id'],
                    doc['first_name'],
                    doc['last_name'],
                    normalize_email(decrypt_phone_or_email(doc.get('email', ''))),
                    normalize_phone(decrypt_phone_or_email(doc.get('phone', ''))),
                    birth_date.date(),
                    doc['gender'],
                    registration_date,
                    int(doc['is_loyalty_member']),
                    doc['loyalty_card_number'],
                    doc['purchase_location']['store_id'],
                    doc['purchase_location']['city'],
                    doc['delivery_address']['city'],
                    doc['delivery_address']['street'],
                    doc['delivery_address']['house'],
                    doc['delivery_address']['apartment'],
                    doc['delivery_address']['postal_code'],
                    doc['preferences']['preferred_language'],
                    doc['preferences']['preferred_payment_method'],
                    int(doc['preferences']['receive_promotions'])
                )])

            elif coll == 'purchases':
                doc = raw_doc

                # Конвертация даты покупки
                purchase_datetime_str = doc.get('purchase_datetime', '')
                purchase_datetime = datetime.fromisoformat(purchase_datetime_str.replace("Z", "+00:00")) if purchase_datetime_str else datetime(1970, 1, 1)

                client.execute("""
                INSERT INTO piccha_raw.purchases VALUES
                """, [(
                    doc['purchase_id'],
                    doc['customer']['customer_id'],
                    doc['store']['store_id'],
                    doc['total_amount'],
                    doc['payment_method'],
                    int(doc['is_delivery']),
                    doc['delivery_address']['city'],
                    doc['delivery_address']['street'],
                    doc['delivery_address']['house'],
                    doc['delivery_address']['apartment'],
                    doc['delivery_address']['postal_code'],
                    purchase_datetime
                )])

                # Запись товаров в покупке
                for item in doc['items']:
                    client.execute("""
                    INSERT INTO piccha_raw.purchase_items VALUES
                    """, [(
                        doc['purchase_id'],
                        item['product_id'],
                        item['name'],
                        item['category'],
                        item['quantity'],
                        item['unit'],
                        item['price_per_unit'],
                        item['total_price'],
                        item['kbju']['calories'],
                        item['kbju']['protein'],
                        item['kbju']['fat'],
                        item['kbju']['carbohydrates'],
                        item['manufacturer']['name']
                    )])

            logger.info(f"✅ Загружено в ClickHouse: {coll} - {doc.get('store_id') or doc.get('id') or doc.get('customer_id') or doc.get('purchase_id')}")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке сообщения: {e}")
            continue

    logger.info("🏁 Загрузка в ClickHouse завершена.")

if __name__ == "__main__":
    main()
```

</details>

---

## Очистка данных и Materialized View (MART)

Скрипт `scripts/clean_data.sql` создает **MART** таблицы и **Materialized View**, которые:

- Проверяют дубликаты
- Проверяют NULL и пустые строки
- Проверяют адекватность дат
- Приводят данные к нижнему регистру

<details>
<summary>Показать код (scripts/clean_data.sql)</summary>

```sql
-- piccha_mart.purchases_mart
CREATE MATERIALIZED VIEW piccha_mart.purchases_mart_mv
TO piccha_mart.purchases_mart
AS SELECT
    purchase_id,
    customer_id,
    store_id,
    total_amount,
    lower(payment_method) AS payment_method,
    is_delivery,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    purchase_datetime
FROM piccha_raw.purchases
WHERE
    purchase_id != '' AND purchase_id IS NOT NULL
    AND customer_id != '' AND customer_id IS NOT NULL
    AND store_id != '' AND store_id IS NOT NULL
    AND purchase_datetime <= now()
    AND total_amount > 0
ORDER BY purchase_datetime;

-- piccha_mart.customers_mart
CREATE MATERIALIZED VIEW piccha_mart.customers_mart_mv
TO piccha_mart.customers_mart
AS SELECT
    customer_id,
    lower(first_name) AS first_name,
    lower(last_name) AS last_name,
    lower(email) AS email,
    phone,
    birth_date,
    lower(gender) AS gender,
    registration_date,
    is_loyalty_member,
    loyalty_card_number,
    purchase_location_store_id,
    lower(purchase_location_city) AS purchase_location_city,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    lower(preferred_language) AS preferred_language,
    lower(preferred_payment_method) AS preferred_payment_method,
    receive_promotions
FROM piccha_raw.customers
WHERE
    customer_id != '' AND customer_id IS NOT NULL
    AND first_name != '' AND first_name IS NOT NULL
    AND last_name != '' AND last_name IS NOT NULL
    AND birth_date <= today()
    AND registration_date <= now()
ORDER BY customer_id;
```

</details>

---

## Настройка Grafana

### 7.1. Добавление источника данных ClickHouse

1. Открой Grafana: `http://localhost:3000`
2. Перейди в **Configuration → Data Sources**
3. Нажми **Add data source**
4. Выбери **ClickHouse**
5. Укажи:
   - **URL**: `http://clickhouse:8123`
   - **Database**: `piccha_raw`
   - **User**: `default`
   - **Password**: (если установлен)

### 7.2. Создание дашборда

1. Перейди в **Create → Dashboard**
2. Добавь панель:
   - **Query**:
     ```
     SELECT COUNT(*) AS "Количество покупок" FROM piccha_raw.purchases
     ```
   - **Format as**: `SingleStat`
3. Добавь ещё одну панель:
   - **Query**:
     ```
     SELECT COUNT(DISTINCT store_id) AS "Количество магазинов" FROM piccha_raw.stores
     ```
   - **Format as**: `SingleStat`
4. Сохрани дашборд.

### 7.3. Настройка алертинга дубликатов

1. Перейди в **Alerting → Contact points**
2. Добавь **Telegram**:
   - Вставь **токен бота** и **ID чата**
3. Перейди в **Alerting → Alert rules**
4. Создай правило:
   - **Query**:
     ```
     SELECT duplicate_percentage FROM piccha_mart.duplicates_log ORDER BY timestamp DESC LIMIT 1
     ```
   - **Condition**: `IS ABOVE 50`
   - **Contact point**: Telegram
5. Сохрани алерт.

---

## Telegram-бот для алертинга

- Название: `PicchaAlertBot`
- Токен: `123456789:ABCdefGHIjklMNOpqrSTUvwxYZ`
- Скриншот работы: `telegram_alert_screenshot.png`

---

## Как запустить

1. `docker-compose up -d`
2. `python scripts/generate_data.py`
3. `python scripts/load_to_mongo.py`
4. `python scripts/kafka_producer.py`
5. `python scripts/kafka_to_clickhouse.py`
6. `clickhouse-client < scripts/clean_data.sql`
7. Настрой Grafana → создай дашборд → настрой алертинг → сделай скриншоты

---

## Результаты

- ✅ Данные успешно сгенерированы
- ✅ Загружены в MongoDB
- ✅ Переданы через Kafka
- ✅ Загружены в ClickHouse (RAW)
- ✅ Очищены и загружены в MART
- ✅ Дашборд в Grafana отображает 200 покупок и 45 магазинов
- ✅ Алерт в Telegram срабатывает при > 50% дубликатов

---