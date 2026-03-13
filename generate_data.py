import pandas as pd
from faker import Faker
import random
import os
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Set up paths
RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 50
NUM_ORDERS = 5000
NUM_REVIEWS = 2000

def generate_customers():
    print("Generating Customers...")
    customers = []
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat()
        })
    df = pd.DataFrame(customers)
    df.to_csv(os.path.join(RAW_DATA_DIR, "customers.csv"), index=False)
    print(f"Saved {len(df)} customers to {RAW_DATA_DIR}/customers.csv")
    return customers

def generate_products():
    print("Generating Products...")
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Toys']
    products = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        products.append({
            "product_id": product_id,
            "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(10.0, 500.0), 2),
            "cost": round(random.uniform(5.0, 300.0), 2)
        })
    df = pd.DataFrame(products)
    df.to_csv(os.path.join(RAW_DATA_DIR, "products.csv"), index=False)
    print(f"Saved {len(df)} products to {RAW_DATA_DIR}/products.csv")
    return products

def generate_orders(customers, products):
    print("Generating Orders...")
    orders = []
    start_date = datetime.now() - timedelta(days=365)
    for order_id in range(1, NUM_ORDERS + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        # Random date within the last year
        order_date = start_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Sometimes order quantities are higher
        quantity = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 10, 3, 2])[0]
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": quantity,
            "total_amount": round(product["price"] * quantity, 2),
            # Introduce some missing/messy data for PySpark cleaning
            "discount_code": random.choice([None, None, "WELCOME10", "SAVE20", None, "HOLIDAY" * random.choice([1, 0])]) or None,
            "status": random.choices(['COMPLETED', 'PENDING', 'CANCELLED', 'RETURNED'], weights=[85, 5, 5, 5])[0]
        })
    df = pd.DataFrame(orders)
    df.to_csv(os.path.join(RAW_DATA_DIR, "orders.csv"), index=False)
    print(f"Saved {len(df)} orders to {RAW_DATA_DIR}/orders.csv")
    return orders

def generate_reviews(customers, products):
    print("Generating Reviews...")
    reviews = []
    for review_id in range(1, NUM_REVIEWS + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        reviews.append({
            "review_id": review_id,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "rating": random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0],
            "review_text": fake.text(max_nb_chars=100) if random.random() > 0.2 else None  # Some reviews have no text
        })
    df = pd.DataFrame(reviews)
    df.to_csv(os.path.join(RAW_DATA_DIR, "reviews.csv"), index=False)
    print(f"Saved {len(df)} reviews to {RAW_DATA_DIR}/reviews.csv")

if __name__ == "__main__":
    cust_data = generate_customers()
    prod_data = generate_products()
    generate_orders(cust_data, prod_data)
    generate_reviews(cust_data, prod_data)
    print("Data generation complete!")
