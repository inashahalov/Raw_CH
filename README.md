# Raw_CH

**Пайплайн для загрузки данных из NoSQL в ClickHouse через Kafka с использованием шифрования персональной информации и визуализацией в Grafana**

---

## Быстрый старт

### Требования
- Docker
- Python 3.8+

### Установка
```bash
git clone git@github.com:inashahalov/Raw_CH.git
cd Raw_CH/project
mkdir -p scripts/sql scripts/
```

---

## Структура проекта

```
project/
├── data/
│   ├── stores/          # Файлы магазинов
│   ├── products/        # Файлы товаров  
│   ├── customers/       # Файлы покупателей
│   └── purchases/       # Файлы покупок
├── scripts/
│   ├── generate_data.py     # Генерация JSON файлов
│   ├── load_to_nosql.py     # Загрузка в MongoDB
│   ├── kafka_producer.py    # Отправка в Kafka
│   └── clickhouse_loader.py # Загрузка в ClickHouse
├── docker-compose.yml       # Docker инфраструктура
├── grafana_dashboard.json   # Дашборд Grafana
└── README.md
```

---

## Этапы реализации

### 1. Генерация JSON файлов
Создает:
- **45 файлов магазинов** (30 больших, 15 маленьких)
- **20 файлов товаров**
- **45 файлов покупателей** (по одному на магазин)
- **200 файлов покупок**

<details>
<summary>Код генерации данных</summary>

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
    " broccoli Овощи и зелень"
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

# === 3. Генерация покупателей (по 1 на магазин) ===
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
        "purchase_location": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "store_type_description": store["store_type_description"],
            "country": store["location"]["country"],
            "city": store["location"]["city"],
            "street": store["location"]["street"],
            "house": store["location"]["house"],
            "postal_code": store["location"]["postal_code"]
        },
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

# === 4. Генерация покупок (200 шт) ===
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
            "email": customer["email"],  # будет зашифровано позже
            "phone": customer["phone"],  # будет зашифровано позже
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

print("Генерация данных завершена: 45 магазинов, 20 товаров, 45 покупателей, 200 покупок.")
```
</details>

---

### 2. Установка зависимостей

```bash
pip install faker pymongo kafka-python clickhouse-driver cryptography
```

---

### 3. Docker инфраструктура

<details>
<summary>docker-compose.yml</summary>

```yaml
version: '3.8'

services:
  mongo:
    image: mongo:latest
    container_name: mongo
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  kafka:
    image: wurstmeister/kafka:2.12-2.8.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    depends_on:
      - zookeeper

  zookeeper:
    image: wurstmeister/zookeeper:3.4.6
    container_name: zookeeper
    ports:
      - "2181:2181"

  clickhouse:
    image: yandex/clickhouse-server
    container_name: clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse

  grafana:
    image: grafana/grafana
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  mongo_data:
  clickhouse_data:
  grafana_data:
```
</details>

**Запуск:**
```bash
docker-compose up -d
```

Убедитесь, что запущены:
- MongoDB
- Kafka
- ClickHouse
- Grafana

---

### 4. Загрузка в MongoDB

<details>
<summary>load_to_nosql.py</summary>

```python
import os
import json
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['piccha']

def load_json_files(directory, collection_name):
    collection = db[collection_name]
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                collection.insert_one(data)

load_json_files('data/stores', 'stores')
load_json_files('data/products', 'products')
load_json_files('data/customers', 'customers')
load_json_files('data/purchases', 'purchases')
```
</details>

---

### 5. Шифрование персональных данных

<details>
<summary>kafka_producer.py (с шифрованием)</summary>

```python
import os
import json
from kafka import KafkaProducer
from cryptography.fernet import Fernet

# Генерация ключа шифрования
key = Fernet.generate_key()
cipher_suite = Fernet(key)

def encrypt_field(field_value):
    if field_value:
        return cipher_suite.encrypt(field_value.encode('utf-8')).decode('utf-8')
    return field_value

def process_and_encrypt_data(data):
    if 'email' in data:
        data['email'] = encrypt_field(data['email'])
    if 'phone' in data:
        data['phone'] = encrypt_field(data['phone'])
    if 'customer' in data and 'email' in data['customer']:
        data['customer']['email'] = encrypt_field(data['customer']['email'])
    if 'customer' in data and 'phone' in data['customer']:
        data['customer']['phone'] = encrypt_field(data['customer']['phone'])
    return data

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def send_to_kafka(directory, topic):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                encrypted_data = process_and_encrypt_data(data.copy())
                producer.send(topic, json.dumps(encrypted_data).encode('utf-8'))

send_to_kafka('data/stores', 'stores')
send_to_kafka('data/products', 'products')
send_to_kafka('data/customers', 'customers')
send_to_kafka('data/purchases', 'purchases')
```
</details>

---

### 6. Загрузка в ClickHouse

<details>
<summary>clickhouse_loader.py</summary>

```python
from kafka import KafkaConsumer
import json
import clickhouse_driver

client = clickhouse_driver.Client(host='localhost')

def consume_from_kafka(topic, table):
    consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        client.execute(f"INSERT INTO {table} FORMAT JSONEachRow", [data])

consume_from_kafka('stores', 'raw_stores')
consume_from_kafka('products', 'raw_products')
consume_from_kafka('customers', 'raw_customers')
consume_from_kafka('purchases', 'raw_purchases')
```
</details>

<details>
<summary>ClickHouse таблицы</summary>

```sql
-- Создание таблиц в ClickHouse
CREATE TABLE raw_stores
(
    store_id String,
    store_name String,
    store_network String,
    store_type_description String,
    type String,
    categories Array(String),
    manager Nested(
        name String,
        phone String,
        email String
    ),
    location Nested(
        country String,
        city String,
        street String,
        house String,
        postal_code String,
        coordinates Nested(
            latitude Float64,
            longitude Float64
        )
    ),
    opening_hours Nested(
        mon_fri String,
        sat String,
        sun String
    ),
    accepts_online_orders UInt8,
    delivery_available UInt8,
    warehouse_connected UInt8,
    last_inventory_date Date
) ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE raw_products
(
    id String,
    name String,
    group String,
    description String,
    kbju Nested(
        calories Float64,
        protein Float64,
        fat Float64,
        carbohydrates Float64
    ),
    price Float64,
    unit String,
    origin_country String,
    expiry_days UInt16,
    is_organic UInt8,
    barcode String,
    manufacturer Nested(
        name String,
        country String,
        website String,
        inn String
    )
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE raw_customers
(
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
    purchase_location Nested(
        country String,
        city String,
        street String,
        house String,
        postal_code String
    ),
    delivery_address Nested(
        country String,
        city String,
        street String,
        house String,
        apartment String,
        postal_code String
    ),
    preferences Nested(
        preferred_language String,
        preferred_payment_method String,
        receive_promotions UInt8
    )
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE raw_purchases
(
    purchase_id String,
    customer Nested(
        customer_id String,
        first_name String,
        last_name String
    ),
    store Nested(
        store_id String,
        store_name String,
        store_network String,
        location Nested(
            country String,
            city String,
            street String,
            house String,
            postal_code String
        )
    ),
    items Array(Nested(
        product_id String,
        name String,
        category String,
        quantity UInt16,
        unit String,
        price_per_unit Float64,
        total_price Float64,
        kbju Nested(
            calories Float64,
            protein Float64,
            fat Float64,
            carbohydrates Float64
        ),
        manufacturer Nested(
            name String,
            country String,
            website String,
            inn String
        )
    )),
    total_amount Float64,
    payment_method String,
    is_delivery UInt8,
    delivery_address Nested(
        country String,
        city String,
        street String,
        house String,
        apartment String,
        postal_code String
    ),
    purchase_datetime DateTime
) ENGINE = MergeTree()
ORDER BY purchase_id;
```
</details>

---

### 7. Визуализация в Grafana

- Подключите Grafana к ClickHouse
- Создайте дашборд с двумя панелями:
  - **Общее количество покупок**: `SELECT count(*) FROM raw_purchases`
  - **Общее количество магазинов**: `SELECT count(*) FROM raw_stores`

---

## Проверка выполнения

- [ ] Все JSON файлы сгенерированы
- [ ] MongoDB заполнена
- [ ] Kafka получает данные
- [ ] ClickHouse получает и хранит RAW JSON
- [ ] Данные зашифрованы
- [ ] Дашборд работает
- [ ] Скриншот дашборда приложен
- [ ] Репозиторий создан, README оформлен

---

## Контакты

**Автор:** Илья Нашахалов
**Telegram** NSilya
