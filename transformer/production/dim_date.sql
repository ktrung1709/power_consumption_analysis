CREATE OR REPLACE PROCEDURE dwh.generate_dim_date()
LANGUAGE plpgsql
AS $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2025-01-01';
    current_date DATE := start_date;
BEGIN
    -- Loop through each date from start_date to end_date
    WHILE current_date <= end_date LOOP
        INSERT INTO dwh.dim_date (date_id, date, day, month, year, quarter)
        VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INT,   -- date_id in the format YYYYMMDD
            current_date,                             -- actual date
            EXTRACT(DAY FROM current_date),           -- day
            EXTRACT(MONTH FROM current_date),         -- month
            EXTRACT(YEAR FROM current_date),          -- year
            EXTRACT(QUARTER FROM current_date)        -- quarter
        );
        
        -- Move to the next day
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
    
    -- Commit the changes
    COMMIT;
END;
$$;