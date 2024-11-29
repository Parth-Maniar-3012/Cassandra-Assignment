import uuid
from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Cassandra connection setup
CASSANDRA_HOST = '127.0.0.1'  # Change to your Cassandra instance host
KEYSPACE = 'medallion_architecture'

# Connect to Cassandra
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

# Create Keyspace and Tables (assuming schema.cql contains the correct schema definitions)
with open('schema.cql', 'r') as file:
    schema = file.read()
session.execute(schema)

# Load data from CSV
df = pd.read_csv('sales_100.csv')

# Insert data into Bronze Table (Batch Insert for Performance)
batch = BatchStatement()
for _, row in df.iterrows():
    batch.add(
        """
        INSERT INTO bronze_sales (id, customer_name, product_name, sales_amount, sales_date, region)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (uuid.uuid4(), row['CustomerName'], row['ProductName'], row['SalesAmount'], 
         datetime.strptime(row['SalesDate'], '%Y-%m-%d'), row['Region'])
    )

session.execute(batch)

# Process data for Silver Table (Data cleaning, removing duplicates)
df_cleaned = df.drop_duplicates()  # Example of data cleaning

# Insert data into Silver Table (Batch Insert for Performance)
batch = BatchStatement()
for _, row in df_cleaned.iterrows():
    batch.add(
        """
        INSERT INTO silver_sales (id, customer_name, product_name, sales_amount, sales_date, region)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (uuid.uuid4(), row['CustomerName'], row['ProductName'], row['SalesAmount'], 
         datetime.strptime(row['SalesDate'], '%Y-%m-%d'), row['Region'])
    )

session.execute(batch)

# Aggregated data for Gold Tables

# Gold Table 1: Sales by Region (Aggregated Data)
region_sales = df_cleaned.groupby('Region')['SalesAmount'].sum().reset_index()

# Insert data into Gold Sales by Region Table (Batch Insert for Performance)
batch = BatchStatement()
for _, row in region_sales.iterrows():
    batch.add(
        """
        INSERT INTO gold_sales_by_region (region, total_sales)
        VALUES (%s, %s)
        """,
        (row['Region'], row['SalesAmount'])
    )

session.execute(batch)

# Gold Table 2: Product Performance (Aggregated Data)
product_performance = df_cleaned.groupby('ProductName')['SalesAmount'].sum().reset_index()

# Insert data into Gold Product Performance Table (Batch Insert for Performance)
batch = BatchStatement()
for _, row in product_performance.iterrows():
    batch.add(
        """
        INSERT INTO gold_product_performance (product_name, total_sales)
        VALUES (%s, %s)
        """,
        (row['ProductName'], row['SalesAmount'])
    )

session.execute(batch)

# Gold Table 3: Customer Purchase Trends (Aggregated Data)
customer_purchases = df_cleaned.groupby('CustomerName')['SalesAmount'].sum().reset_index()

# Insert data into Gold Customer Purchases Table (Batch Insert for Performance)
batch = BatchStatement()
for _, row in customer_purchases.iterrows():
    batch.add(
        """
        INSERT INTO gold_customer_purchases (customer_name, total_spent)
        VALUES (%s, %s)
        """,
        (row['CustomerName'], row['SalesAmount'])
    )

session.execute(batch)

# Take screenshots of Gold tables (Example: Retrieve and Print Data from Gold Tables)
gold_tables = ['gold_sales_by_region', 'gold_product_performance', 'gold_customer_purchases']
for table in gold_tables:
    rows = session.execute(f"SELECT * FROM {table};")
    print(f"\nData from {table}:")
    for row in rows:
        print(row)

# Close connection
cluster.shutdown()
