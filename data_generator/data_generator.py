import csv
import random
from faker import Faker
import os

fake = Faker()

# Directory creater code 
os.makedirs("generated_data/customers_data", exist_ok=True)
os.makedirs("generated_data/products_data", exist_ok=True)
os.makedirs("generated_data/orders_data", exist_ok=True)

# Data generator code
def generate_customers(filename, start_id, rows=300):
    with open(filename, "w", newline="") as f:
        current_id = start_id
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "customer_phone", "customer_email"])
        for i in range(rows):
            writer.writerow([
                f"CUST_{current_id:04d}",
                fake.name() if random.random() > 0.1 else "",
                fake.msisdn() if random.random() > 0.15 else "abc123",
                fake.email() if random.random() > 0.1 else ""
            ])
            current_id = current_id + 1
    return current_id

def generate_products(filename, start_id, rows=300):
    current_id = start_id
    categories = ["Electronics", "Furniture", "Clothing", "Books"]
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "product_category", "product_price"])
        for i in range(rows):
            writer.writerow([
                f"PROD_{current_id:04d}",
                fake.word() if random.random() > 0.1 else "",
                random.choice(categories),
                random.choice([random.randint(100, 100000), "abc", -999])
            ])
            current_id = current_id + 1
    return current_id

def generate_orders(filename, start_id, rows=300):
    current_id = start_id
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "quantity", "total_amount"])
        for i in range(rows):
            writer.writerow([
                f"ORD_{current_id:04d}",
                f"CUST_{random.randint(1,350):04d}",
                f"PROD_{random.randint(1,350):04d}",
                random.choice([1,2,3,-1]),
                random.choice([random.randint(100,100000), "abc"])
            ])
            current_id = current_id + 1
    return current_id


# Driver code 
cust_id = 1
prod_id = 1
ord_id = 1
for i in range(10):
    cust_id = generate_customers(
        f"generated_data/customers_data/customers_part_{i}.csv",
        cust_id
    )

    prod_id = generate_products(
        f"generated_data/products_data/products_part_{i}.csv",
        prod_id
    )

    ord_id = generate_orders(
        f"generated_data/orders_data/orders_part_{i}.csv",
        ord_id
    )
