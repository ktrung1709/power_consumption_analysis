create or replace view dwh.daily_total_consumption as (
    select d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    group by d.day, d.month, d.year
);

create or replace view dwh.daily_consumption_by_customer as (
    select c.customer_id, c.customer_name, c.customer_type, d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    group by c.customer_id, c.customer_name, c.customer_type, d.day, d.month, d.year
);

create or replace view dwh.daily_consumption_by_customer_type as (
    select c.customer_type, d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    group by c.customer_type, d.day, d.month, d.year
);

create or replace view dwh.daily_consumption_by_location as (
    select m.location, m.iso_code, d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_electric_meter m on f.meter_id = m.meter_id
    group by m.location, m.iso_code, d.day, d.month, d.year
);

create or replace view dwh.monthly_total_revenue as (
    select d.month, d.year, sum(f.revenue) as revenue
    from dwh.fact_revenue f
    inner join dwh.dim_date d on f.date_id = d.date_id
    group by d.month, d.year
);

create or replace view dwh.monthly_revenue_by_customer as (
    select c.customer_id, c.customer_name, c.customer_type, d.month, d.year, sum(f.revenue) as revenue
    from dwh.fact_revenue f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    group by c.customer_id, c.customer_name, c.customer_type, d.month, d.year
);

create or replace view dwh.monthly_revenue_by_customer_type as (
    select c.customer_type, d.month, d.year, sum(f.revenue) as revenue
    from dwh.fact_revenue f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    group by c.customer_type, d.month, d.year
);

create or replace view dwh.daily_consumption_by_meter as (
    select c.customer_id, c.customer_name, m.meter_id, d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    inner join dwh.dim_electric_meter m on f.meter_id = m.meter_id
    group by c.customer_id, c.customer_name, m.meter_id, d.day, d.month, d.year
);

create or replace view dwh.daily_consumption_by_time_of_day as (
    select c.customer_id, c.customer_name, m.meter_id, f.time_of_day, d.day, d.month, d.year, sum(f.consumption) as consumption
    from dwh.fact_power_consumption f
    inner join dwh.dim_date d on f.date_id = d.date_id
    inner join dwh.dim_customer c on f.customer_id = c.customer_id
    inner join dwh.dim_electric_meter m on f.meter_id = m.meter_id
    group by c.customer_id, c.customer_name, m.meter_id, d.day, d.month, d.year, f.time_of_day
);
