# scripts/kafka_to_clickhouse.py

from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, cast
from datetime import datetime
from decimal import Decimal
from clickhouse_driver import Client
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Ключ шифрования (вставьте сюда ключ из kafka_producer.py) ===
# !!! ВАЖНО: Этот ключ ДОЛЖЕН быть таким же, как в kafka_producer.py !!!
ENCRYPTION_KEY = b'S_kG1-1EwjfkFUfukpPaHGZ92KlWKkdlpLLVeskPeEM='
cipher = Fernet(ENCRYPTION_KEY)

logger.info("🔑 Используем ключ шифрования.")


# === Нормализация и дешифровка ===
def decrypt_phone_or_email(value: str | None) -> str:
    if not value:
        return ""
    try:
        # Пытаемся расшифровать. Если не удается, возвращаем как есть.
        return cipher.decrypt(value.encode()).decode()
    except Exception as e:
        logger.debug(f"Не удалось расшифровать значение '{value[:10]}...': {e}")
        return value


def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    # Простая нормализация, можно усложнить при необходимости
    return phone.strip()


def normalize_email(email: str | None) -> str:
    """Нормализует email: приводит к нижнему регистру и удаляет пробелы."""
    return email.strip().lower() if email else ""


# === Обработка дат ===
def parse_iso_datetime(date_str: str | None) -> datetime:
    """Парсит ISO формат даты-времени, включая 'Z'. Возвращает datetime или_epoch_ при ошибке."""
    if not date_str:
        logger.warning("Получена пустая строка даты. Используется 1970-01-01.")
        return datetime(1970, 1, 1)
    try:
        # fromisoformat может справиться с 'Z' в конце, но иногда лучше заменить
        if date_str.endswith('Z'):
            # Заменяем 'Z' на '+00:00' для явного указания UTC
            dt = datetime.fromisoformat(date_str[:-1] + '+00:00')
        else:
            dt = datetime.fromisoformat(date_str)
        return dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты '{date_str}': {e}. Используется 1970-01-01.")
        return datetime(1970, 1, 1)


def parse_iso_date(
        date_str: str | None) -> datetime:  # Возвращаем datetime, так как ClickHouse ожидает Date, но питонский date легче получить из datetime.date()
    """Парсит ISO формат даты (YYYY-MM-DD). Возвращает datetime или_epoch_ при ошибке."""
    if not date_str:
        logger.warning("Получена пустая строка даты. Используется 1970-01-01.")
        return datetime(1970, 1, 1)
    try:
        # Предполагаем, что формат YYYY-MM-DD
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты '{date_str}': {e}. Используется 1970-01-01.")
        return datetime(1970, 1, 1)


# === Подключение к ClickHouse ===
# Используем порт 9000, как настроено в docker-compose.yml
client = Client(host='localhost', port=9000)


# === Создание таблиц RAW ===
def create_raw_tables() -> None:
    client.execute("CREATE DATABASE IF NOT EXISTS piccha_raw")

    # Таблица магазинов
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

    # Таблица продуктов
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

    # Таблица покупателей
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

    # Таблица покупок (плоская, по одной строке на товар в чеке)
    # Структура взята из DESCRIBE piccha_raw.purchases
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.purchases (
        purchase_id String,
        customer_id String,
        product_id String,
        quantity Int32,
        price Decimal(10, 2),
        purchase_date DateTime
    ) ENGINE = MergeTree() ORDER BY (purchase_id, product_id)
    """)

    # Таблица элементов покупки (опционально, если нужна детализация)
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
        auto_offset_reset='earliest',  # Читаем с начала, если новых сообщений нет
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
                logger.debug(f"Обработка store: {doc.get('store_id')}")

                last_inventory_date_dt = parse_iso_date(doc.get('last_inventory_date'))

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
                    normalize_phone(decrypt_phone_or_email(doc['manager'].get('phone'))),
                    doc['manager']['email'],  # Предполагаем, что email менеджера не шифруется
                    doc['location']['country'],
                    doc['location']['city'],
                    doc['location']['street'],
                    doc['location']['house'],
                    doc['location']['postal_code'],
                    float(doc['location']['coordinates']['latitude']),
                    float(doc['location']['coordinates']['longitude']),
                    doc['opening_hours']['mon_fri'],
                    doc['opening_hours']['sat'],
                    doc['opening_hours']['sun'],
                    int(doc['accepts_online_orders']),
                    int(doc['delivery_available']),
                    int(doc['warehouse_connected']),
                    last_inventory_date_dt.date()  # Вставка Date
                )])
                logger.info(f"✅ Загружено в ClickHouse: stores - {doc['store_id']}")

            elif coll == 'products':
                doc = raw_doc
                logger.debug(f"Обработка product: {doc.get('id')}")

                client.execute("""
                INSERT INTO piccha_raw.products VALUES
                """, [(
                    doc['id'],
                    doc['name'],
                    doc['group'],
                    doc['description'],
                    float(doc['kbju']['calories']),
                    float(doc['kbju']['protein']),
                    float(doc['kbju']['fat']),
                    float(doc['kbju']['carbohydrates']),
                    float(doc['price']),
                    doc['unit'],
                    doc['origin_country'],
                    int(doc['expiry_days']),
                    int(doc['is_organic']),
                    doc['barcode'],
                    doc['manufacturer']['name'],
                    doc['manufacturer']['country'],
                    doc['manufacturer']['website'].strip(),  # Убираем лишние пробелы
                    doc['manufacturer']['inn']
                )])
                logger.info(f"✅ Загружено в ClickHouse: products - {doc['id']}")

            elif coll == 'customers':
                doc = raw_doc
                logger.debug(f"Обработка customer: {doc.get('customer_id')}")

                birth_date_dt = parse_iso_date(doc.get('birth_date'))
                registration_date_dt = parse_iso_datetime(doc.get('registration_date'))

                client.execute("""
                INSERT INTO piccha_raw.customers VALUES
                """, [(
                    doc['customer_id'],
                    doc['first_name'],
                    doc['last_name'],
                    normalize_email(decrypt_phone_or_email(doc.get('email'))),
                    normalize_phone(decrypt_phone_or_email(doc.get('phone'))),
                    birth_date_dt.date(),  # Вставка Date
                    doc['gender'],
                    registration_date_dt,  # Вставка DateTime
                    int(doc['is_loyalty_member']),
                    doc['loyalty_card_number'],
                    doc['purchase_location']['store_id'],
                    doc['purchase_location']['city'],
                    doc['delivery_address']['city'],
                    doc['delivery_address']['street'],
                    doc['delivery_address']['house'],
                    doc['delivery_address'].get('apartment', ''),  # Может отсутствовать
                    doc['delivery_address']['postal_code'],
                    doc['preferences']['preferred_language'],
                    doc['preferences']['preferred_payment_method'],
                    int(doc['preferences']['receive_promotions'])
                )])
                logger.info(f"✅ Загружено в ClickHouse: customers - {doc['customer_id']}")

            elif coll == 'purchases':
                doc = raw_doc
                logger.debug(f"Обработка purchase: {doc.get('purchase_id')}")

                purchase_datetime_dt = parse_iso_datetime(doc.get('purchase_datetime'))

                # Предполагаем, что таблица piccha_raw.purchases ожидает плоские записи
                # по одной на каждый товар (item) в заказе.
                for item in doc.get('items', []):
                    client.execute("""
                    INSERT INTO piccha_raw.purchases VALUES
                    """, [(
                        doc['purchase_id'],
                        doc['customer']['customer_id'],
                        item['product_id'],  # product_id из item
                        int(item['quantity']),  # quantity из item
                        Decimal(str(item['price_per_unit'])),  # price_per_unit из item -> Decimal(10,2)
                        purchase_datetime_dt  # purchase_date из общей даты покупки
                    )])

                    # Также вставляем в detail таблицу purchase_items
                    client.execute("""
                    INSERT INTO piccha_raw.purchase_items VALUES
                    """, [(
                        doc['purchase_id'],
                        item['product_id'],
                        item['name'],
                        item['category'],
                        int(item['quantity']),
                        item['unit'],
                        float(item['price_per_unit']),
                        float(item['total_price']),
                        float(item['kbju']['calories']),
                        float(item['kbju']['protein']),
                        float(item['kbju']['fat']),
                        float(item['kbju']['carbohydrates']),
                        item['manufacturer']['name']
                    )])

                logger.info(
                    f"✅ Загружено в ClickHouse: purchases - {doc['purchase_id']} ({len(doc.get('items', []))} items)")

            else:
                logger.warning(f"Неизвестная коллекция: {coll}")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке сообщения: {e}", exc_info=True)  # exc_info=True для полного трейса
            # Продолжаем обработку следующих сообщений, не останавливаемся на ошибке
            continue

    logger.info("🏁 Загрузка в ClickHouse завершена (потребитель остановлен).")


if __name__ == "__main__":
    main()