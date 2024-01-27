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

# Запит для витягування максимальної суми угоди для конкретного покупця
max_amount_query = func.max(Sale.amount).filter(Sale.customer_id == specific_customer_id)
max_amount = session.query(max_amount_query).scalar()

# Виведення результату
print(f"Максимальна сума угоди для покупця з ID {specific_customer_id}: {max_amount}")

# Ідентифікатор конкретного покупця
specific_customer_id = 1  # Замініть на ідентифікатор покупця, якого ви шукаєте

# Запит для витягування мінімальної суми угоди для конкретного покупця
min_amount_query = func.min(Sale.amount).filter(Sale.customer_id == specific_customer_id)
min_amount = session.query(min_amount_query).scalar()

# Виведення результату
print(f"Мінімальна сума угоди для покупця з ID {specific_customer_id}: {min_amount}")



# Залишаємо консоль відкритою, очікуючи введення користувача
input("Натисніть Enter для завершення...")

