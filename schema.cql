-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS medallion_architecture WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the Keyspace
USE medallion_architecture;

-- Create Bronze Table
CREATE TABLE IF NOT EXISTS bronze_sales (
    id UUID PRIMARY KEY,
    customer_name TEXT,
    product_name TEXT,
    sales_amount DOUBLE,
    sales_date DATE,
    region TEXT
);

-- Create Silver Table
CREATE TABLE IF NOT EXISTS silver_sales (
    id UUID PRIMARY KEY,
    customer_name TEXT,
    product_name TEXT,
    sales_amount DOUBLE,
    sales_date DATE,
    region TEXT
);

-- Create Gold Table 1: Sales by Region
CREATE TABLE IF NOT EXISTS gold_sales_by_region (
    region TEXT PRIMARY KEY,
    total_sales DOUBLE
);

-- Create Gold Table 2: Product Performance
CREATE TABLE IF NOT EXISTS gold_product_performance (
    product_name TEXT PRIMARY KEY,
    total_sales DOUBLE
);

-- Create Gold Table 3: Customer Purchases
CREATE TABLE IF NOT EXISTS gold_customer_purchases (
    customer_name TEXT PRIMARY KEY,
    total_spent DOUBLE
);
