"""
generate_kaggle_dataset.py
Generates a realistic 2,200,000+ record Kaggle-compliant E-Commerce dataset with 19 columns.
Includes real-world data anomalies (missing values, whitespace, duplicates, casing)
to test and validate our Pandas ETL cleaning and chunking pipeline.
"""

import os
import time
import numpy as np
import pandas as pd

def generate_ecommerce_dataset(output_path="data/raw/ecommerce_transactions_raw.csv", total_records=2_200_000, batch_size=200_000):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    print(f"[*] Starting dataset generation: {total_records:,} records into '{output_path}'...")
    start_time = time.time()

    countries = ["United States", "United Kingdom", "Germany", "France", "Canada", "Australia", "Japan", "Brazil"]
    cities = {
        "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
        "Germany": ["Berlin", "Munich", "Frankfurt", "Hamburg", "Cologne"],
        "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
        "Canada": ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa"],
        "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
        "Japan": ["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Sapporo"],
        "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"]
    }

    categories = ["Electronics", "Home & Kitchen", "Apparel", "Books", "Health & Beauty", "Sports & Outdoors"]
    products_by_cat = {
        "Electronics": ["Wireless Headphones", "Smartwatch Pro", "4K Ultra Monitor", "USB-C Hub", "Mechanical Keyboard"],
        "Home & Kitchen": ["Air Fryer Deluxe", "Espresso Machine", "Ceramic Cookware Set", "Robot Vacuum", "Blender 1000W"],
        "Apparel": ["Cotton Hoodie", "Running Shoes", "Denim Jacket", "Thermal Base Layer", "Leather Belt"],
        "Books": ["Big Data Analytics Guide", "Database Architecture Handbook", "Python for Enterprise", "Cloud Computing Patterns", "Machine Learning Mastery"],
        "Health & Beauty": ["Electric Toothbrush", "Vitamin C Serum", "Facial Cleanser", "Hair Dryer Ionic", "Sunscreen SPF 50"],
        "Sports & Outdoors": ["Yoga Mat Non-Slip", "Adjustable Dumbbells", "Camping Tent 4-Person", "Cycling Helmet", "Hydration Water Bottle"]
    }

    payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Bank Transfer", "Klarna"]
    order_statuses = ["Completed", "Shipped", "Delivered", "Pending", "Cancelled", "Refunded"]
    shipping_carriers = ["FedEx Express", "UPS Ground", "DHL Global", "USPS Priority", "Amazon Logistics"]

    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore"]

    write_header = True
    rows_generated = 0

    # Base timestamp: 2024-01-01 to 2026-10-01
    base_ts = pd.Timestamp("2024-01-01").value // 10**9
    max_ts = pd.Timestamp("2026-10-01").value // 10**9

    while rows_generated < total_records:
        current_batch = min(batch_size, total_records - rows_generated)
        
        # Vectorized generation
        txn_ids = [f"TXN-{rows_generated + i + 1:08d}" for i in range(current_batch)]
        order_ids = [f"ORD-{((rows_generated + i) // 2) + 1:07d}" for i in range(current_batch)]
        cust_num = np.random.randint(1, 250_000, size=current_batch)
        cust_ids = [f"CUST-{cn:06d}" for cn in cust_num]

        fn = np.random.choice(first_names, size=current_batch)
        ln = np.random.choice(last_names, size=current_batch)
        cust_names = [f"{f} {l}" for f, l in zip(fn, ln)]
        
        # Email with 2% intentional nulls / missing values
        email_domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "corporate.org"]
        ed = np.random.choice(email_domains, size=current_batch)
        cust_emails = [f"{f.lower()}.{l.lower()}{cn%1000}@{d}" for f, l, cn, d in zip(fn, ln, cust_num, ed)]
        null_email_mask = np.random.rand(current_batch) < 0.02
        cust_emails = [None if is_null else e for is_null, e in zip(null_email_mask, cust_emails)]

        country_indices = np.random.choice(len(countries), size=current_batch)
        batch_countries = [countries[idx] for idx in country_indices]
        batch_cities = [cities[c][np.random.randint(0, len(cities[c]))] for c in batch_countries]

        cat_indices = np.random.choice(len(categories), size=current_batch)
        batch_cats = [categories[idx] for idx in cat_indices]
        batch_prods = [products_by_cat[cat][np.random.randint(0, len(products_by_cat[cat]))] for cat in batch_cats]
        prod_ids = [f"PRD-{hash(p) % 10000:04d}" for p in batch_prods]

        unit_prices = np.round(np.random.uniform(12.50, 899.99, size=current_batch), 2)
        quantities = np.random.randint(1, 6, size=current_batch)
        discount_rates = np.round(np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20, 0.30], p=[0.45, 0.20, 0.15, 0.10, 0.07, 0.03], size=current_batch), 2)
        shipping_fees = np.round(np.random.choice([0.0, 4.99, 9.99, 14.99, 24.99], p=[0.30, 0.35, 0.20, 0.10, 0.05], size=current_batch), 2)
        
        subtotals = np.round(unit_prices * quantities * (1.0 - discount_rates), 2)
        tax_amounts = np.round(subtotals * 0.08, 2) # 8% avg sales tax
        total_amounts = np.round(subtotals + shipping_fees + tax_amounts, 2)

        batch_payments = np.random.choice(payment_methods, size=current_batch)
        batch_statuses = np.random.choice(order_statuses, p=[0.60, 0.15, 0.12, 0.05, 0.05, 0.03], size=current_batch)
        batch_carriers = np.random.choice(shipping_carriers, size=current_batch)

        # Timestamps
        rand_seconds = np.random.randint(base_ts, max_ts, size=current_batch)
        timestamps = pd.to_datetime(rand_seconds, unit='s').strftime('%Y-%m-%d %H:%M:%S')

        # Intentional dirty data injection for ETL cleaning credit (whitespace, casing)
        dirty_status_mask = np.random.rand(current_batch) < 0.03
        batch_statuses = [f"  {s.lower()}  " if is_dirty else s for is_dirty, s in zip(dirty_status_mask, batch_statuses)]

        df_batch = pd.DataFrame({
            "transaction_id": txn_ids,
            "order_id": order_ids,
            "customer_id": cust_ids,
            "customer_name": cust_names,
            "customer_email": cust_emails,
            "customer_country": batch_countries,
            "customer_city": batch_cities,
            "product_id": prod_ids,
            "product_category": batch_cats,
            "product_name": batch_prods,
            "unit_price": unit_prices,
            "quantity": quantities,
            "discount_rate": discount_rates,
            "tax_amount": tax_amounts,
            "shipping_cost": shipping_fees,
            "total_amount": total_amounts,
            "payment_method": batch_payments,
            "order_status": batch_statuses,
            "shipping_carrier": batch_carriers,
            "transaction_timestamp": timestamps
        })

        # Inject 0.5% duplicate rows periodically to test deduplication
        if rows_generated > 0 and rows_generated % 500_000 == 0:
            dup_sample = df_batch.iloc[:1000]
            df_batch = pd.concat([df_batch, dup_sample], ignore_index=True)

        df_batch.to_csv(output_path, mode='a', header=write_header, index=False)
        write_header = False
        rows_generated += current_batch
        print(f" -> Generated {rows_generated:,} / {total_records:,} records...")

    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"[+] Dataset generation complete! Total rows: {rows_generated:,} | File size: {file_size_mb:.2f} MB | Time: {elapsed:.2f}s")

if __name__ == "__main__":
    generate_ecommerce_dataset()

