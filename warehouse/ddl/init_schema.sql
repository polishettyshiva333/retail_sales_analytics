
CREATE DATABASE IF NOT EXISTS RETAIL_SALES_DATA;

USE DATABASE RETAIL_SALES_DATA;

CREATE SCHEMA IF NOT EXISTS SALES_WAREHOUSE;

USE SCHEMA SALES_WAREHOUSE;


--DROP TABLE IF EXISTS fact_sales;
--DROP TABLE IF EXISTS dim_order;
--DROP TABLE IF EXISTS dim_customer;
--DROP TABLE IF EXISTS dim_product;
--DROP TABLE IF EXISTS dim_date;



CREATE TABLE IF NOT EXISTS dim_order (
    order_key INT PRIMARY KEY,
    order_id STRING,
    line_number INT,
    quantity INT,
    total_price FLOAT,
    order_status STRING,
    order_date DATE, 
    deal_size STRING   
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT PRIMARY KEY,
    product_id STRING,
    product_line INT,
    price_each FLOAT,
    msrp FLOAT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT PRIMARY KEY,
    customer_name VARCHAR,
    phone VARCHAR,
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    city STRING,
    state STRING,
    country STRING,
    postal_code VARCHAR,
    contact_first_name VARCHAR,
    contact_last_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    order_date DATE,
    date_val INT,
    month_val VARCHAR,
    year_val INT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key INT AUTOINCREMENT PRIMARY KEY,
    order_key INT,
    product_key INT,
    customer_key INT,
    date_key INT,
    quantity INT,
    total_price FLOAT,

    FOREIGN KEY (order_key) REFERENCES dim_order(order_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    
);