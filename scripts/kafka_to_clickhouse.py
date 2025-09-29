# scripts/kafka_to_clickhouse.py

from __future__ import annotations
import json
import logging
from typing import Any, Dict, List
from datetime import datetime
from decimal import Decimal
from clickhouse_driver import Client
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Вывод в консоль
        # logging.FileHandler('kafka_to_clickhouse.log') # Опционально: вывод в файл
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# === ВАЖНО: ВСТАВЬТЕ СЮДА КЛЮЧ ИЗ ВЫВОДА kafka_producer.py ====================
# === ПРИМЕР: b'DpY1VLwmMeRO18y5BTLIibyzd-BheI5N1NpxCoHdMBo=' =================
# === !!! УБЕДИТЕСЬ, ЧТО КЛЮЧ СОВПАДАЕТ С ТЕМ, ЧТО В kafka_producer.py !!! ====
# =============================================================================
ENCRYPTION_KEY = b'dI6S2mer6xD6JpDcATx5r_wFE78Rf1TQC6bVg29ZjKE='
cipher = Fernet(ENCRYPTION_KEY)

logger.info("🔑 Используем ключ шифрования.")


# === Нормализация и дешифровка ===
def decrypt_phone_or_email(value: str | None) -> str:
    """
    Пытается расшифровать значение. Если не удается, возвращает как есть.
    """
    if not value:
        return ""
    try:
        decrypted_value = cipher.decrypt(value.encode()).decode()
        logger.debug(f"Успешно расшифровано значение.")
        return decrypted_value
    except Exception as e:
        logger.debug(f"Не удалось расшифровать значение: {e}. Возвращено как есть.")
        return value  # fallback: вернуть как есть


def normalize_phone(phone: str | None) -> str:
    """
    Нормализует телефонный номер: приводит к формату +7XXXXXXXXXX.
    """
    if not phone:
        return ""
    # Простая нормализация: убираем все, кроме цифр
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return f"+{digits}"
    # Если формат не стандартный, возвращаем оригинал
    logger.debug(f"Телефон '{phone}' не соответствует стандартному формату.")
    return phone


def normalize_email(email: str | None) -> str:
    """
    Нормализует email: приводит к нижнему регистру и удаляет пробелы.
    """
    if not email:
        return ""
    return email.strip().lower()


# === Обработка дат ===
def safe_parse_datetime(datetime_str: str | None) -> datetime:
    """
    Безопасно парсит строку даты-времени в формате ISO. Возвращает datetime или epoch.
    """
    if not datetime_str:
        logger.warning("Получена пустая строка даты-времени. Используется 1970-01-01T00:00:00.")
        return datetime(1970, 1, 1)
    try:
        # fromisoformat может справиться с 'Z' в конце, заменим на +00:00 для надежности
        if datetime_str.endswith('Z'):
            parsed_dt = datetime.fromisoformat(datetime_str[:-1] + '+00:00')
        else:
            parsed_dt = datetime.fromisoformat(datetime_str)
        return parsed_dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты-времени '{datetime_str}': {e}. Используется 1970-01-01T00:00:00.")
        return datetime(1970, 1, 1)


def safe_parse_date(
        date_str: str | None) -> datetime:  # Возвращаем datetime для совместимости, используем .date() при вставке
    """
    Безопасно парсит строку даты (YYYY-MM-DD). Возвращает datetime или epoch.
    """
    if not date_str:
        logger.warning("Получена пустая строка даты. Используется 1970-01-01.")
        return datetime(1970, 1, 1)
    try:
        # Предполагаем, что формат YYYY-MM-DD
        parsed_dt = datetime.strptime(date_str, "%Y-%m-%d")
        return parsed_dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты '{date_str}': {e}. Используется 1970-01-01.")
        return datetime(1970, 1, 1)


# === Подключение к ClickHouse ===
# Используем порт 9000, как настроено в docker-compose.yml
client = Client(host='localhost', port=9000)


# === Создание таблиц RAW ===
def create_raw_tables() -> None:
    """
    Создает базу данных piccha_raw и все необходимые таблицы, если они еще не существуют.
    Типы ID изменены на String.
    """
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
    """
    Основная функция: создает таблицы, подключается к Kafka, читает сообщения и вставляет данные в ClickHouse.
    """
    logger.info("🚀 Запуск kafka_to_clickhouse consumer...")

    # Создаем таблицы
    try:
        create_raw_tables()
        logger.info("✅ Таблицы piccha_raw созданы или уже существуют.")
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблиц: {e}")
        return  # Останавливаем выполнение, если таблицы не созданы

    # Подключаемся к Kafka
    try:
        consumer = KafkaConsumer(
            'piccha_raw',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Читаем с начала, если новых сообщений нет
            enable_auto_commit=True,
            group_id='clickhouse-loader-v2',  # Изменен group_id для избежания проблем с предыдущими состояниями
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # session_timeout_ms=45000, # Увеличение таймаута сессии, если потребитель медленный
            # heartbeat_interval_ms=3000 # Интервал heartbeat
        )
        logger.info("✅ Подключение к Kafka установлено.")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Kafka: {e}")
        return

    logger.info("⏳ Ожидаю данные из Kafka топика 'piccha_raw'...")

    # Основной цикл обработки сообщений
    for message in consumer:
        try:
            # Получаем JSON-документ из сообщения
            raw_doc: Dict[str, Any] = message.value
            collection_type: str = raw_doc.pop('_collection', 'unknown')

            logger.debug(f"📥 Получено сообщение для коллекции: {collection_type}")

            # Обработка в зависимости от типа коллекции
            if collection_type == 'stores':
                doc = raw_doc

                # Обработка last_inventory_date
                last_inventory_date_dt = safe_parse_date(doc.get('last_inventory_date'))

                # Вставка в таблицу stores
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

            elif collection_type == 'products':
                doc = raw_doc

                # Вставка в таблицу products
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
                    doc['manufacturer']['website'].strip(),
                    doc['manufacturer']['inn']
                )])
                logger.info(f"✅ Загружено в ClickHouse: products - {doc['id']}")

            elif collection_type == 'customers':
                doc = raw_doc

                # Обработка дат
                birth_date_dt = safe_parse_date(doc.get('birth_date'))
                registration_date_dt = safe_parse_datetime(doc.get('registration_date'))

                # Вставка в таблицу customers
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

            elif collection_type == 'purchases':
                doc = raw_doc

                # Обработка общей даты покупки
                purchase_datetime_dt = safe_parse_datetime(doc.get('purchase_datetime'))

                # Предполагаем, что таблица piccha_raw.purchases ожидает плоские записи
                # по одной на каждый товар (item) в заказе.
                items_list = doc.get('items', [])
                inserted_items_count = 0
                for item in items_list:
                    try:
                        # --- Вставка в основную таблицу покупок (по одной строке на товар) ---
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

                        # --- Вставка в детальную таблицу элементов покупки ---
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
                        inserted_items_count += 1

                    except KeyError as ke:
                        logger.error(
                            f"❌ Отсутствует обязательное поле в item '{item.get('product_id', 'UNKNOWN')}': {ke}")
                        # Пропускаем этот item, но продолжаем обработку других
                        continue
                    except Exception as ie:
                        logger.error(f"❌ Ошибка при обработке item '{item.get('product_id', 'UNKNOWN')}': {ie}")
                        continue

                logger.info(
                    f"✅ Загружено в ClickHouse: purchases - {doc['purchase_id']} ({inserted_items_count} items)")

            else:
                logger.warning(f"⚠️  Неизвестная коллекция: {collection_type}")

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при обработке сообщения: {e}", exc_info=True)
            # Продолжаем обработку следующих сообщений, не останавливаемся на ошибке
            continue

    logger.info("🏁 Consumer остановлен.")


if __name__ == "__main__":
    main()