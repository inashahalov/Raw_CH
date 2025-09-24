# scripts/kafka_to_clickhouse.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, TypedDict, cast

from clickhouse_driver import Client
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Генерация ключа шифрования ===
ENCRYPTION_KEY = Fernet.generate_key()
cipher = Fernet(ENCRYPTION_KEY)
logger.info(f"🔑 Ключ шифрования (сохраните!): {ENCRYPTION_KEY.decode()}")


def encrypt_field(value: str | None) -> str:
    """Шифрует строковое поле. Возвращает пустую строку, если значение None или пустое."""
    if not value:
        return ""
    return cipher.encrypt(value.encode()).decode()


def normalize_phone(phone: str | None) -> str:
    """Приводит номер телефона к формату +7XXXXXXXXXX."""
    if not phone:
        return ""
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return f"+{digits}"
    return phone  # fallback на оригинальный формат


def normalize_email(email: str | None) -> str:
    """Нормализует email: приводит к нижнему регистру и удаляет пробелы."""
    return email.strip().lower() if email else ""


# === Типы для валидации структуры данных из Kafka ===
class ManagerDict(TypedDict):
    name: str
    phone: str
    email: str


class CoordinatesDict(TypedDict):
    latitude: float
    longitude: float


class LocationDict(TypedDict):
    country: str
    city: str
    street: str
    house: str
    postal_code: str
    coordinates: CoordinatesDict


class OpeningHoursDict(TypedDict):
    mon_fri: str
    sat: str
    sun: str


class StoreDocument(TypedDict):
    store_id: str
    store_name: str
    store_network: str
    store_type_description: str
    type: str
    categories: List[str]
    manager: ManagerDict
    location: LocationDict
    opening_hours: OpeningHoursDict
    accepts_online_orders: bool
    delivery_available: bool
    warehouse_connected: bool
    last_inventory_date: str  # YYYY-MM-DD


class KBJUDict(TypedDict):
    calories: float
    protein: float
    fat: float
    carbohydrates: float


class ManufacturerDict(TypedDict):
    name: str
    country: str
    website: str
    inn: str


class ProductDocument(TypedDict):
    id: str
    name: str
    group: str
    description: str
    kbju: KBJUDict
    price: float
    unit: str
    origin_country: str
    expiry_days: int
    is_organic: bool
    barcode: str
    manufacturer: ManufacturerDict


class PurchaseLocationDict(TypedDict):
    store_id: str
    store_name: str
    store_network: str
    store_type_description: str
    country: str
    city: str
    street: str
    house: str
    postal_code: str


class DeliveryAddressDict(TypedDict):
    country: str
    city: str
    street: str
    house: str
    apartment: str
    postal_code: str


class PreferencesDict(TypedDict):
    preferred_language: str
    preferred_payment_method: str
    receive_promotions: bool


class CustomerDocument(TypedDict):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    birth_date: str  # ISO format
    gender: str
    registration_date: str  # ISO format
    is_loyalty_member: bool
    loyalty_card_number: str
    purchase_location: PurchaseLocationDict
    delivery_address: DeliveryAddressDict
    preferences: PreferencesDict


class PurchaseItemDict(TypedDict):
    product_id: str
    name: str
    category: str
    quantity: int
    unit: str
    price_per_unit: float
    total_price: float
    kbju: KBJUDict
    manufacturer: ManufacturerDict


class StoreRefDict(TypedDict):
    store_id: str
    store_name: str
    store_network: str
    location: LocationDict


class CustomerRefDict(TypedDict):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    is_loyalty_member: bool
    loyalty_card_number: str


class PurchaseDocument(TypedDict):
    purchase_id: str
    customer: CustomerRefDict
    store: StoreRefDict
    items: List[PurchaseItemDict]
    total_amount: float
    payment_method: str
    is_delivery: bool
    delivery_address: DeliveryAddressDict
    purchase_datetime: str  # ISO format


# === Подключение к ClickHouse ===
client = Client(host='localhost', port=9000)  # Используем порт 9000, как настроено в docker-compose.yml


# === Создание таблиц ===
def create_tables() -> None:
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
        kbju_calories Float64,
        kbju_protein Float64,
        kbju_fat Float64,
        kbju_carbohydrates Float64,
        price Float64,
        unit String,
        origin_country String,
        expiry_days Int32,
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
        email_encrypted String,
        phone_encrypted String,
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
        total_amount Float64,
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
        name String,
        category String,
        quantity Int32,
        unit String,
        price_per_unit Float64,
        total_price Float64,
        kbju_calories Float64,
        kbju_protein Float64,
        kbju_fat Float64,
        kbju_carbohydrates Float64,
        manufacturer_name String
    ) ENGINE = MergeTree() ORDER BY (purchase_id, product_id)
    """)


# === Основной consumer ===
def main() -> None:
    global doc
    create_tables()

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
                doc = cast(StoreDocument, raw_doc)
                categories = doc.get('categories', [])
                if not isinstance(categories, list):
                    categories = [str(categories)]

                client.execute("""
                INSERT INTO piccha_raw.stores VALUES
                """, [(
                    doc['store_id'],
                    doc['store_name'],
                    doc['store_network'],
                    doc['store_type_description'],
                    doc['type'],
                    categories,
                    doc['manager']['name'],
                    encrypt_field(normalize_phone(doc['manager']['phone'])),
                    encrypt_field(normalize_email(doc['manager']['email'])),
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
                    doc['last_inventory_date']
                )])

            elif coll == 'products':
                doc = cast(ProductDocument, raw_doc)
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
                doc = cast(CustomerDocument, raw_doc)
                client.execute("""
                INSERT INTO piccha_raw.customers VALUES
                """, [(
                    doc['customer_id'],
                    doc['first_name'],
                    doc['last_name'],
                    encrypt_field(normalize_email(doc['email'])),
                    encrypt_field(normalize_phone(doc['phone'])),
                    doc['birth_date'],
                    doc['gender'],
                    doc['registration_date'],
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
                doc = cast(PurchaseDocument, raw_doc)
                # Основная запись покупки
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
                    doc['purchase_datetime']
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