


# scripts/load_to_mongo.py
from pymongo import MongoClient
import json
import os

# Подключается к MongoDB (порт 27018, как указано в docker-compose)
client = MongoClient('mongodb://localhost:27018/')
db = client['piccha_db']

# Очищает коллекции перед загрузкой
# Загружает JSON-файлы из соответствующих директорий в нужные коллекции
collections = {
    'stores': 'data/stores/',
    'products': 'data/products/',
    'customers': 'data/customers/',
    'purchases': 'data/purchases/'
}

for coll_name, folder in collections.items():
    collection = db[coll_name]
    collection.delete_many({})  # очистка
    files = [f for f in os.listdir(folder) if f.endswith('.json')]
    for file in files:
        with open(os.path.join(folder, file), 'r', encoding='utf-8') as f:
            data = json.load(f)
            collection.insert_one(data)
    print(f"✅ Загружено {len(files)} документов в коллекцию '{coll_name}'")