# Завдання 2
# Додайте механізми для оновлення, видалення та вставки
# даних до бази даних за допомогою інтерфейсу меню. Користувач не може ввести запити INSERT, UPDATE, DELETE
# безпосередньо. Забороніть можливість оновлення та видалення
# усіх даних для кожної таблиці (UPDATE та DELETE без умов).

from sqlalchemy import create_engine, MetaData, Table, insert, update, delete
from sqlalchemy.sql import select
import psycopg2
import json

# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['user']
db_password = config['password']

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/Hospital'
engine = create_engine(db_url)
# з'єднання з БД
conn = engine.connect()
metadata = MetaData()
# завантаження таблиць
# автоматичне завантаження
metadata.reflect(bind=engine)
# або одна табличка
# departments = Table('departments', metadata, autoload=True, autoload_with=engine)

# Нова функція для перевірки чи є умова для UPDATE та DELETE
def is_condition_needed(table):
    columns = table.columns.keys()
    print("Доступні колонки для визначення умови: ")
    for idx, column in enumerate(columns, start=1):
        print(f"{idx}.{column}")
    selected_column_idx = int(input("Введіть номер колонки для умови: "))

    if 1 <= selected_column_idx <= len(columns):
        condition_column = columns[selected_column_idx - 1]
    else:
        print("Невірний номер колонки! Визначення умови відмінено!")
        return False

    condition_value = input(f"Введіть значення для умови, {condition_column}: ")
    return True, condition_column, condition_value

def insert_row(table: metadata):
    columns = table.columns.keys()

    values = {}
    for column in columns:
        value = input(f"Введіть значення для колонки {column}: ")
        values[column] = value
    query = insert(table).values(values)
    conn.execute(query)
    conn.commit()

    print("Рядок успішно додано!")

def update_rows(table):
    condition_needed, condition_column, condition_value = is_condition_needed(table)
    if condition_needed:
        new_values = {}
        columns = table.columns.keys()
        for column in columns:
            value = input(f"Введіть значення для колонки {column}: ")
            new_values[column] = value

        confirm_update = input("Оновити усі рядки? у/п? ")
        if confirm_update.lower() == 'y':
            query = update(table).where(getattr(table.c, condition_column) == condition_value).values(new_values)
            conn.execute(query)
            conn.commit()

def delete_rows(table):
    condition_needed, condition_column, condition_value = is_condition_needed(table)
    if condition_needed:
        confirm_update = input("Видалити усі рядки з цієї таблиці? у/п? ")
        if confirm_update.lower() == 'y':
            query = delete(table).where(getattr(table.c, condition_column) == condition_value)
            conn.execute(query)
            conn.commit()

while True:
    print("Оберіть таблицю: ")
    for table_name in metadata.tables.keys():
        print(table_name)
    table_name = input("Введіть назву таблиці або 0, щоб вийти ")
    if table_name == '0':
        break
    # перевіримо, чи існує таблиця
    if table_name in metadata.tables:
        table = metadata.tables[table_name]
        print(f"Ви обрали таблицю {table_name}")

        print("1. Вставити рядки")
        print("2. Оновити рядки")
        print("3. Видалити рядки")
        print("0. Вийти")

        choice = input("Оберіть опцію: ")

        if choice == "1":
            insert_row(table)
        elif choice == "2":
            update_rows(table)
        elif choice == "3":
            delete_rows(table)
        elif choice == "0":
            break
        else:
            print("Невірний вибір. Будь ласка, оберіть знову.")
    else:
        print("Такої таблиці не існує. Будь ласка, введіть правильну назву.")
