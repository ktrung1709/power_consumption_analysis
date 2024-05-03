create schema serving

create table serving.daily_consumption_by_customer (
    customer_id INT,
    customer_name VARCHAR(255),
    date DATE,
    consumption FLOAT8,
    PRIMARY KEY (customer_id, date)
)

create table serving.daily_consumption_by_customer_type (
    customer_type VARCHAR(20),
    date DATE,
    consumption FLOAT8,
    PRIMARY KEY (customer_type, date)
)

create table serving.daily_consumption_by_location (
    location VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    date DATE,
    consumption FLOAT8,
    PRIMARY KEY (location, date)
)

create table serving.monthly_consumption_by_customer (
    customer_id INT,
    customer_name VARCHAR(255),
    month int,
    year int,
    consumption FLOAT8,
    PRIMARY KEY (customer_id, month, year)
)

create table serving.monthly_consumption_by_customer_type (
    customer_type VARCHAR(20),
    month int,
    year int,
    consumption FLOAT8,
    PRIMARY KEY (customer_type, month, year)
)

create table serving.monthly_consumption_by_location (
    location VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    month int,
    year int,
    consumption FLOAT8,
    PRIMARY KEY (location, month, year)
)

create table serving.daily_total_consumption (
    "date" DATE PRIMARY KEY,
    consumption FLOAT8
)

create table serving.monthly_total_consumption (
    month int,
    year int,
    consumption FLOAT8,
    PRIMARY KEY (month, year)
)