# Завдання 2
# Додайте механізми для оновлення, видалення та вставки
# даних до бази даних за допомогою інтерфейсу меню. Користувач не може ввести запити INSERT, UPDATE, DELETE
# безпосередньо. Забороніть можливість оновлення та видалення
# усіх даних для кожної таблиці (UPDATE та DELETE без умов).

from sqlalchemy import create_engine, MetaData, Table, insert, update, delete, select
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import json
from datetime import datetime

# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['user']
db_password = config['password']

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/Hospital'
engine = create_engine(db_url)
# З'єднання з БД
conn = engine.connect()
metadata = MetaData()
# Завантаження таблиць
metadata.reflect(bind=engine)

def insert_row(table):
    try:
        columns = table.columns.keys()

        values = {}
        for col in columns:
            value = input(f"Введіть значення для колонки {col}: ")
            values[col] = value

        query = insert(table).values(values)
        conn.execute(query)
        conn.commit()

        print("Рядок успішно додано!")
    except SQLAlchemyError as e:
        print(f"Помилка при вставці рядка: {e}")

def update_rows(table):
    try:
        columns = table.columns.keys()
        print("Доступні колонки для оновлення: ")
        for idx, column in enumerate(columns, start=1):
            print(f"{idx}.{column}")
        selected_column_idx = int(input("Введіть номер колонки для оновлення: "))

        if 1 <= selected_column_idx <= len(columns):
            condition_column = columns[selected_column_idx - 1]
        else:
            print("Невірний номер колонки!")

        condition_value = input(f"Введіть значення для умови, {condition_column}: ")
        new_values = {}
        for column in columns:
            value = input(f"Введіть значення для колонки {column}: ")
            new_values[column] = value

        confirm_update = input("Оновити усі рядки? у/п? ")
        if confirm_update.lower() == 'y':
            query = update(table).where(getattr(table.c, condition_column) == condition_value).values(new_values)
            conn.execute(query)
            conn.commit()

            print("Рядки успішно оновлено!")
    except SQLAlchemyError as e:
        print(f"Помилка при оновленні рядків: {e}")

def delete_rows(table):
    try:
        columns = table.columns.keys()
        print("Доступні колонки для видалення: ")
        for idx, column in enumerate(columns, start=1):
            print(f"{idx}.{column}")
        selected_column_idx = int(input("Введіть номер колонки для умови видалення: "))

        if 1 <= selected_column_idx <= len(columns):
            condition_column = columns[selected_column_idx - 1]
        else:
            print("Невірний номер колонки! Видалення відмінено!")

        condition_value = input(f"Введіть значення для умови, {condition_column}: ")

        confirm_delete = input("Видалити усі рядки з цієї таблиці? у/п? ")
        if confirm_delete.lower() == 'y':
            query = delete(table).where(getattr(table.c, condition_column) == condition_value)
            conn.execute(query)
            conn.commit()

            print("Рядки успішно видалено!")
    except SQLAlchemyError as e:
        print(f"Помилка при видаленні рядків: {e}")

def display_all_rows(table):
    try:
        query = select(table)
        result = conn.execute(query).fetchall()

        print(f"\nУсі рядки у таблиці {table.name}:\n")
        for row in result:
            print(row)
    except SQLAlchemyError as e:
        print(f"Помилка при виведенні усіх рядків: {e}")

while True:
    print("\nОберіть таблицю: ")
    for table_name in metadata.tables.keys():
        print(table_name)
    table_name = input("Введіть назву таблиці або 0, щоб вийти: ")
    if table_name == '0':
        break
    # Перевіримо, чи існує таблиця
    if table_name in metadata.tables:
        table = metadata.tables[table_name]
        print(f"Ви обрали таблицю {table_name}")

        print("1. Вставити рядки")
        print("2. Оновити рядки")
        print("3. Видалити рядки")
        print("4. Переглянути всі рядки")
        print("0. Вийти")

        choice = input("Оберіть опцію: ")

        if choice == "1":
            insert_row(table)
        elif choice == "2":
            update_rows(table)
        elif choice == "3":
            delete_rows(table)
        elif choice == "4":
            display_all_rows(table)
        elif choice == "0":
            break
        else:
            print("Невірний вибір. Будь ласка, оберіть знову.")
    else:
        print("Такої таблиці не існує. Будь ласка, введіть правильну назву.")
