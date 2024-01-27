# Завдання 3
# Додайте до першого завдання можливість збереження
# результатів фільтрів у файл. Шлях і назву файлу вкажіть у
# налаштуваннях програми.


from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey, select, func
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import json
from datetime import datetime

# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['user']
db_password = config['password']

# Шлях та назва файлу для збереження результатів фільтрів
result_file_path = config.get('result_file_path', 'filter_results.json')

# З'єднання з базою даних
db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/Sales'
engine = create_engine(db_url)

# Оголошення базового класу
Base = declarative_base()

# Оголошення моделі для таблиці Sales
class Sale(Base):
    __tablename__ = 'sales'

    id = Column(Integer, Sequence('sale_id_seq'), primary_key=True)
    amount = Column(Integer)
    date = Column(Date)
    salesman_id = Column(Integer, ForeignKey('salesmen.id'))
    customer_id = Column(Integer, ForeignKey('customers.id'))

# Оголошення моделі для таблиці Salesmen
class Salesman(Base):
    __tablename__ = 'salesmen'

    id = Column(Integer, Sequence('salesman_id_seq'), primary_key=True)
    name = Column(String(50))
    contact_number = Column(String(15))
    sales = relationship('Sale', back_populates='salesman')

# Оголошення моделі для таблиці Customers
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, Sequence('customer_id_seq'), primary_key=True)
    name = Column(String(50))
    email = Column(String(50))
    address = Column(String(100))
    sales = relationship('Sale', back_populates='customer')

# Встановлення зв'язку між Sale та Salesman/Customer
Sale.salesman = relationship('Salesman', back_populates='sales')
Sale.customer = relationship('Customer', back_populates='sales')

# Створення таблиць у базі даних
Base.metadata.create_all(engine)

# Створення сесії для взаємодії з базою даних
Session = sessionmaker(bind=engine)
session = Session()

def save_results_to_file(results, file_path):
    with open(file_path, 'w') as file:
        json.dump(results, file, indent=2)
    print(f"Результати збережено у файл: {file_path}")

def load_results_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            results = json.load(file)
        return results
    except FileNotFoundError:
        print(f"Файл не знайдено: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Помилка при читанні файлу: {file_path}")
        return None

# Функція для виведення результатів
def display_results(results):
    if results:
        for sale in results:
            print(f"ID: {sale['id']}, Amount: {sale['amount']}, Date: {sale['date']}, "
                  f"Salesman ID: {sale['salesman_id']}, Customer ID: {sale['customer_id']}")
    else:
        print("Результати не знайдено.")

# Запит для витягування усіх угод
query_all_sales = select(Sale)
all_sales = session.execute(query_all_sales).fetchall()

# Виведення та збереження результатів
display_results(all_sales)
save_results_to_file(all_sales, result_file_path)



# Закриття сесії
session.close()
