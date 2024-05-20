-- Step 1: Join necessary tables and filter the data
CREATE VIEW a as (
WITH industrial_consumption AS (
    SELECT 
        f.customer_id, 
        f.contract_id, 
        f.meter_id, 
        d.month, 
        d.year, 
        f.time_of_day, 
        m.voltage, 
        f.consumption,
        '01' AS day  -- Step 2: Add a constant day value
    FROM 
        dwh.fact_power_consumption f
        INNER JOIN dwh.dim_date d ON f.date_id = d.date_id
        INNER JOIN dwh.dim_customer c ON c.customer_id = f.customer_id
        INNER JOIN dwh.dim_electric_meter m ON m.meter_id = f.meter_id
    WHERE 
        c.customer_type = 'industrial' 
),

-- Step 2: Add the date_id column
industrial_consumption_with_date AS (
    SELECT 
        customer_id, 
        contract_id, 
        meter_id, 
        time_of_day, 
        voltage, 
        consumption,
        year,
        month,
        (year || LPAD(month::varchar, 2, '0') || '01')::int AS date_id
    FROM 
        industrial_consumption
),

-- Step 3: Aggregate the consumption data
monthly_industrial_consumption AS (
    SELECT
        customer_id,
        contract_id,
        meter_id,
        date_id,
        time_of_day,
        voltage,
        SUM(consumption) AS consumption
    FROM 
        industrial_consumption_with_date
    GROUP BY 
        customer_id, 
        contract_id, 
        meter_id, 
        date_id, 
        time_of_day, 
        voltage
),

-- Step 4: Join with industrial_price and calculate the revenue
industrial_revenue AS (
    SELECT
        mic.customer_id,
        mic.contract_id,
        mic.meter_id,
        mic.date_id,
        mic.time_of_day,
        mic.voltage,
        mic.consumption,
        ip.price,
        (mic.consumption * ip.price) AS time_of_day_revenue
    FROM 
        monthly_industrial_consumption mic
        INNER JOIN cmis.industrial_price ip 
        ON mic.voltage = ip.voltage_tier 
        AND mic.time_of_day = ip.time_of_day
),

-- Step 5: Aggregate the revenue data
final_industrial_revenue AS (
    SELECT
        customer_id,
        contract_id,
        meter_id,
        date_id,
        SUM(time_of_day_revenue) AS revenue
    FROM 
        industrial_revenue
    GROUP BY 
        customer_id, 
        contract_id, 
        meter_id, 
        date_id
)

SELECT 
    date_id, 
    customer_id, 
    contract_id, 
    meter_id, 
    revenue
FROM 
    final_industrial_revenue
)

INSERT INTO dwh.fact_revenue (date_id, customer_id, contract_id, meter_id, revenue)
    SELECT 
        date_id, 
        customer_id, 
        contract_id, 
        meter_id, 
        revenue
    FROM 
        a;