CREATE OR REPLACE VIEW c as (
WITH consumption_data AS (
    SELECT 
        f.customer_id,
        f.contract_id,
        f.meter_id,
        d.month,
        d.year,
        f.consumption,
        (d.year || LPAD(d.month::VARCHAR, 2, '0') || '01')::INTEGER AS date_id
    FROM 
        dwh.fact_power_consumption f
        INNER JOIN dwh.dim_date d ON f.date_id = d.date_id
        INNER JOIN dwh.dim_customer c ON c.customer_id = f.customer_id
    WHERE 
        c.customer_type = 'residential' 
),
aggregated_consumption AS (
    SELECT
        customer_id,
        contract_id,
        meter_id,
        date_id,
        SUM(consumption) AS consumption
    FROM
        consumption_data
    GROUP BY
        customer_id, 
        contract_id, 
        meter_id, 
        date_id
),

calculated_revenue AS (
    SELECT 
        ac.customer_id,
        ac.contract_id,
        ac.meter_id,
        ac.date_id,
        ac.consumption,
        CASE 
            WHEN ac.consumption <= 50 THEN ac.consumption * CAST(rp.price[0] AS FLOAT8)
            WHEN ac.consumption <= 100 THEN 50 * CAST(rp.price[0] AS FLOAT8) + (ac.consumption - 50) * CAST(rp.price[1] AS FLOAT8)
            WHEN ac.consumption <= 200 THEN 50 * CAST(rp.price[0] AS FLOAT8) + 50 * CAST(rp.price[1] AS FLOAT8) + (ac.consumption - 100) * CAST(rp.price[2] AS FLOAT8)
            WHEN ac.consumption <= 300 THEN 50 * CAST(rp.price[0] AS FLOAT8) + 50 * CAST(rp.price[1] AS FLOAT8) + 100 * CAST(rp.price[2] AS FLOAT8) + (ac.consumption - 200) * CAST(rp.price[3] AS FLOAT8)
            WHEN ac.consumption <= 400 THEN 50 * CAST(rp.price[0] AS FLOAT8) + 50 * CAST(rp.price[1] AS FLOAT8) + 100 * CAST(rp.price[2] AS FLOAT8) + 100 * CAST(rp.price[3] AS FLOAT8) + (ac.consumption - 300) * CAST(rp.price[4] AS FLOAT8)
            ELSE 50 * CAST(rp.price[0] AS FLOAT8) + 50 * CAST(rp.price[1] AS FLOAT8) + 100 * CAST(rp.price[2] AS FLOAT8) + 100 * CAST(rp.price[3] AS FLOAT8) + 100 * CAST(rp.price[4] AS FLOAT8) + (ac.consumption - 400) * CAST(rp.price[5] AS FLOAT8)
        END AS revenue
    FROM 
        aggregated_consumption ac,
        (SELECT SPLIT_TO_ARRAY(LISTAGG(price, ',') WITHIN GROUP (ORDER BY tier), ',') as price
            FROM cmis.residential_price ) rp
)
SELECT 
    date_id, 
    customer_id, 
    contract_id, 
    meter_id, 
    revenue
FROM 
    calculated_revenue
)

INSERT INTO dwh.fact_revenue (date_id, customer_id, contract_id, meter_id, revenue)
    SELECT 
        date_id, 
        customer_id, 
        contract_id, 
        meter_id, 
        revenue
    FROM 
        c;