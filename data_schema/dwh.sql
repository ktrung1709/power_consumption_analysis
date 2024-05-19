CREATE SCHEMA dwh;

CREATE TABLE dwh.dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    phone VARCHAR(255) unique,
    email VARCHAR(255) unique,
    address VARCHAR(255),
    customer_type VARCHAR(20),
    updated_time TIMESTAMP 
);

CREATE TABLE dwh.dim_contract (
    contract_id INT PRIMARY KEY,
    date_created DATE,
    status varchar(20),
    updated_time TIMESTAMP
);

CREATE TABLE dwh.dim_electric_meter (
    meter_id INT PRIMARY KEY,
    meter_type varchar(50),
    voltage varchar(50),
    status varchar(50),
    location VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    updated_time TIMESTAMP
);

CREATE TABLE dwh.dim_date (
    date_id INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT
)

CREATE TABLE dwh.fact_power_consumption (
    date_id INT REFERENCES dwh.dim_date(date_id),
    customer_id INT REFERENCES dwh.dim_customer(customer_id),
    contract_id INT REFERENCES dwh.dim_contract(contract_id),
    meter_id INT REFERENCES dwh.dim_electric_meter(meter_id),
    time_of_day VARCHAR(50),
    consumption FLOAT8
);

CREATE TABLE dwh.fact_revenue (
    date_id INT REFERENCES dwh.dim_date(date_id),
    customer_id INT REFERENCES dwh.dim_customer(customer_id),
    contract_id INT REFERENCES dwh.dim_contract(contract_id),
    meter_id INT REFERENCES dwh.dim_electric_meter(meter_id),
    revenue FLOAT8
)