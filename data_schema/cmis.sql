create schema cmis;

CREATE TABLE cmis.customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    phone VARCHAR(255) unique,
    email VARCHAR(255) unique,
    address VARCHAR(255),
    customer_type VARCHAR(20)
);

CREATE TABLE cmis.contract (
    contract_id INT PRIMARY KEY,
    customer_id INT unique,
    date_created DATE,
    status varchar(20),
    FOREIGN KEY (customer_id) REFERENCES cmis.customer(customer_id)
);

CREATE TABLE cmis.electric_meter (
    meter_id INT PRIMARY KEY,
    contract_id INT,
    meter_type varchar(50),
    voltage varchar(50),
    status varchar(50),
    location VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    FOREIGN KEY (contract_id) REFERENCES cmis.contract(contract_id)
);

create table cmis.residential_price (
	tier int primary key,
	description varchar(255),
	price float8
);

insert into cmis.residential_price (tier, description, price) values (1, '0-50 kWh', 1806);
insert into cmis.residential_price (tier, description, price) values (2, '51-100 kWh', 1866);
insert into cmis.residential_price (tier, description, price) values (3, '101-200 kWh', 2167);
insert into cmis.residential_price (tier, description, price) values (4, '201-300 kWh', 2729);
insert into cmis.residential_price (tier, description, price) values (5, '301-400 kWh', 3050);
insert into cmis.residential_price (tier, description, price) values (6, 'More than 401 kWh', 3151);

create table cmis.commercial_price (
	voltage_tier varchar(50),
	time_of_day varchar(50),
	price float8,
	primary key (voltage_tier, time_of_day)
);

INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'normal', 2870);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'low', 1746);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'high', 4937);

INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'normal', 2830);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'low', 1666);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'high', 4736);

INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('22kV and above', 'normal', 2629);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('22kV and above', 'low', 1465);
INSERT INTO cmis.commercial_price (voltage_tier, time_of_day, price) VALUES('22kV and above', 'high', 4575);

create table cmis.industrial_price (
	voltage_tier varchar(50),
	time_of_day varchar(50),
	price float8,
	primary key (voltage_tier, time_of_day)
);

INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'normal', 1809);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'low', 1184);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('less than 6kV', 'high', 3314);

INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'normal', 1729);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'low', 1124);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('6kV to less than 22kV', 'high', 3194);

INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('22kV to less than 100kV', 'normal', 1669);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('22kV to less than 100kV', 'low', 1084);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('22kV to less than 100kV', 'high', 3093);

INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('100kV and above', 'normal', 1649);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('100kV and above', 'low', 1044);
INSERT INTO cmis.industrial_price (voltage_tier, time_of_day, price) VALUES('100kV and above', 'high', 2973);