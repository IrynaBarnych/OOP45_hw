# Завдання 1
# Створіть тритабличну базу даних Sales (Продажі). У цій
# базі даних мають бути таблиці: Sales (інформація про конкретні
# продажі), Salesmen (інформація про продавців), Customers (інформація про покупців).


from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
import json

# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['user']
db_password = config['password']

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

# Оголошення моделі для таблиці Customers
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, Sequence('customer_id_seq'), primary_key=True)
    name = Column(String(50))
    email = Column(String(50))
    address = Column(String(100))

# Створення таблиць у базі даних
Base.metadata.create_all(engine)
