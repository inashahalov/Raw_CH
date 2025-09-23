# Raw_CH
пайплайн для загрузки данных из NoSQL в ClickHouse через Kafka с использованием шифрования персональной информации и визуализацией в Grafana.

Для выполнения данного задания, давайте разберем его поэтапно:

### Шаг 1: Генерация JSON файлов

Мы уже имеем скрипт для генерации необходимых JSON файлов. Этот скрипт создаст 45 файлов магазинов, 20 файлов продуктов, минимум одного покупателя в каждом магазине и 200 покупок. Убедитесь, что этот скрипт работает корректно и сохраняет файлы в нужные директории.

### Шаг 2: Настройка NoSQL хранилища

Для демонстрации мы можем использовать MongoDB в Docker. Создадим `docker-compose.yml` файл для запуска MongoDB и других сервисов.

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

### Шаг 3: Загрузка JSON файлов в NoSQL (MongoDB)

Создадим скрипт на Python для загрузки данных в MongoDB.

```python
import os
import json
from pymongo import MongoClient

client = MongoClient('mongodb://mongo:27017/')
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

### Шаг 4: Настройка Kafka и загрузка данных в ClickHouse

Создадим несколько топиков Kafka для каждой коллекции MongoDB и скрипт для отправки данных в Kafka.

```bash
# Создание топиков Kafka
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic stores --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic products --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic customers --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic purchases --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

Скрипт для отправки данных в Kafka:

```python
import os
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def send_to_kafka(directory, topic):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                producer.send(topic, json.dumps(data).encode('utf-8'))

send_to_kafka('data/stores', 'stores')
send_to_kafka('data/products', 'products')
send_to_kafka('data/customers', 'customers')
send_to_kafka('data/purchases', 'purchases')
```

Создадим таблицы в ClickHouse и потребителя Kafka для загрузки данных.

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

Скрипт для потребителя Kafka:

```python
from kafka import KafkaConsumer
import json
import clickhouse_driver

client = clickhouse_driver.Client(host='clickhouse')

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

### Шаг 5: Шифрование персональной информации

Используем библиотеку `cryptography` для шифрования телефонов и электронных адресов.

```python
from cryptography.fernet import Fernet

key = Fernet.generate_key()
cipher_suite = Fernet(key)

def encrypt_data(data):
    return cipher_suite.encrypt(data.encode('utf-8')).decode('utf-8')

def decrypt_data(data):
    return cipher_suite.decrypt(data.encode('utf-8')).decode('utf-8')

# Пример шифрования
encrypted_email = encrypt_data("alexey.ivanov@example.com")
encrypted_phone = encrypt_data("+7-900-123-45-67")

print(encrypted_email)
print(encrypted_phone)
```

Измените скрипт загрузки данных в ClickHouse, чтобы шифровать телефоны и электронные адреса.

### Шаг 6: Настройка Grafana

1. Откройте Grafana по адресу `http://localhost:3000`.
2. Добавьте источник данных ClickHouse.
3. Создайте дашборд с двумя панелями:
   - Количество магазинов: `SELECT count(*) FROM raw_stores`
   - Количество покупок: `SELECT count(*) FROM raw_purchases`

### Шаг 7: Проверка

Убедитесь, что все данные загружены правильно и дашборд отображает правильные числа.

### Шаг 8: Репозиторий

Создайте Git репозиторий и добавьте все необходимые файлы и скрипты.

```
git init
git add .
git commit -m "Initial commit"
git remote add origin <your-repo-url>
git push -u origin master
```


