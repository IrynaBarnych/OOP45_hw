from sqlalchemy import create_engine, Column, Integer, String, Sequence, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import select, func
import json

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
all_sales_query = select(Sale)
all_sales_result = session.execute(all_sales_query).fetchall()
print("\nВсі угоди:")
for sale in all_sales_result:
    print(f"ID: {sale.id}, Amount: {sale.amount}, Date: {sale.date}, Salesman ID: {sale.salesman_id}, Customer ID: {sale.customer_id}")

# Запит для витягування ID продавця з максимальною сумою продажів
max_salesman_query = session.query(Sale.salesman_id, func.sum(Sale.amount).label('total_sales')) \
    .group_by(Sale.salesman_id) \
    .order_by(func.sum(Sale.amount).desc()) \
    .limit(1)

# Виконання запиту та отримання результату
max_salesman_result = max_salesman_query.first()

if max_salesman_result:
    max_salesman_id, total_sales = max_salesman_result
    print(f"\nПродавець з максимальною сумою продажів (ID {max_salesman_id}): {total_sales}")
else:
    print("\nІнформація не знайдена.")

# Виведення результату
print("\nМаксимальна сума угоди:")
max_amount_query = func.max(Sale.amount)
max_amount_result = session.query(max_amount_query).scalar()
print(f"Максимальна сума угоди: {max_amount_result}")

# Додайте інші звіти за аналогією для решти запитів, які ви хочете реалізувати.

# Закриття сесії
session.close()

# Залишаємо консоль відкритою, очікуючи введення користувача
input("\nНатисніть Enter для завершення...")
