from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import select
import json
from sqlalchemy import func


# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['user']
db_password = config['password']

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

# Запит для витягування усіх угод
query = select(Sale)
result = session.execute(query).fetchall()

# Виведення результатів
for sale in result:
    print(f"ID: {sale.id}, Amount: {sale.amount}, Date: {sale.date}, Salesman ID: {sale.salesman_id}, "
          f"Customer ID: {sale.customer_id}")

# Закриття сесії
session.close()

# Ідентифікатор конкретного продавця
specific_salesman_id = 1  # Замініть на ідентифікатор продавця, якого ви шукаєте

# Запит для витягування угод конкретного продавця
query = select(Sale).where(Sale.salesman_id == specific_salesman_id)
result = session.execute(query).fetchall()

# Виведення результатів
for sale in result:
    print(f"ID: {sale.id}, Amount: {sale.amount}, Date: {sale.date}, Salesman ID: {sale.salesman_id}, "
          f"Customer ID: {sale.customer_id}")

# Запит для витягування максимальної суми угоди
query = func.max(Sale.amount)
max_amount = session.query(query).scalar()

# Виведення результату
print(f"Максимальна сума угоди: {max_amount}")


# Запит для витягування мінімальної суми угоди
min_amount_query = func.min(Sale.amount)
min_amount = session.query(min_amount_query).scalar()
# Виведення результату
print(f"Мінімальна сума угоди: {min_amount}")


# Ідентифікатор конкретного продавця
specific_salesman_id = 1  # Замініть на ідентифікатор продавця, якого ви шукаєте

# Запит для витягування максимальної суми угоди для конкретного продавця
max_amount_query = func.max(Sale.amount).filter(Sale.salesman_id == specific_salesman_id)
max_amount = session.query(max_amount_query).scalar()

# Виведення результату
print(f"Максимальна сума угоди для продавця з ID {specific_salesman_id}: {max_amount}")


# Ідентифікатор конкретного продавця
specific_salesman_id = 1  # Замініть на ідентифікатор продавця, якого ви шукаєте

# Запит для витягування мінімальної суми угоди для конкретного продавця
min_amount_query = func.min(Sale.amount).filter(Sale.salesman_id == specific_salesman_id)
min_amount = session.query(min_amount_query).scalar()

# Виведення результату
print(f"Мінімальна сума угоди для продавця з ID {specific_salesman_id}: {min_amount}")


# Ідентифікатор конкретного покупця
specific_customer_id = 1  # Замініть на ідентифікатор покупця, якого ви шукаєте

from sqlalchemy import func

# Запит для витягування ID продавця з максимальною сумою продажів
max_salesman_query = session.query(Sale.salesman_id, func.sum(Sale.amount).label('total_sales')) \
    .group_by(Sale.salesman_id) \
    .order_by(func.sum(Sale.amount).desc()) \
    .limit(1)

# Виконання запиту та отримання результату
max_salesman_result = max_salesman_query.first()

if max_salesman_result:
    max_salesman_id, total_sales = max_salesman_result
    print(f"Продавець з максимальною сумою продажів (ID {max_salesman_id}): {total_sales}")
else:
    print("Інформація не знайдена.")

# Виведення результату
print(f"Максимальна сума угоди для покупця з ID {specific_customer_id}: {max_amount}")

from sqlalchemy import func

# Запит для витягування ID продавця з мінімальною сумою продажів
min_salesman_query = session.query(Sale.salesman_id, func.sum(Sale.amount).label('total_sales')) \
    .group_by(Sale.salesman_id) \
    .order_by(func.sum(Sale.amount).asc()) \
    .limit(1)

# Виконання запиту та отримання результату
min_salesman_result = min_salesman_query.first()

if min_salesman_result:
    min_salesman_id, total_sales = min_salesman_result
    print(f"Продавець з мінімальною сумою продажів (ID {min_salesman_id}): {total_sales}")
else:
    print("Інформація не знайдена.")

from sqlalchemy import func

# Запит для витягування ID покупця з максимальною сумою покупок
max_customer_query = session.query(Sale.customer_id, func.sum(Sale.amount).label('total_purchases')) \
    .group_by(Sale.customer_id) \
    .order_by(func.sum(Sale.amount).desc()) \
    .limit(1)

# Виконання запиту та отримання результату
max_customer_result = max_customer_query.first()

if max_customer_result:
    max_customer_id, total_purchases = max_customer_result
    print(f"Покупець з максимальною сумою покупок (ID {max_customer_id}): {total_purchases}")
else:
    print("Інформація не знайдена.")

from sqlalchemy import func

# Ідентифікатор конкретного покупця
specific_customer_id = 1  # Замініть на ідентифікатор покупця, для якого ви шукаєте середню суму покупок

# Запит для витягування середньої суми покупок для конкретного покупця
avg_purchase_query = session.query(func.avg(Sale.amount).label('avg_purchase')) \
    .filter(Sale.customer_id == specific_customer_id)

# Виконання запиту та отримання результату
avg_purchase_result = avg_purchase_query.first()

if avg_purchase_result:
    avg_purchase = avg_purchase_result[0]
    print(f"Середня сума покупок для покупця (ID {specific_customer_id}): {avg_purchase}")
else:
    print("Інформація не знайдена.")


# Залишаємо консоль відкритою, очікуючи введення користувача
input("Натисніть Enter для завершення...")

