CREATE OR REPLACE PROCEDURE dwh.update_dim_electric_meter()
LANGUAGE plpgsql
AS $$
DECLARE
    meter_cursor CURSOR FOR
        SELECT meter_id, meter_type, voltage, status, location, latitude, longitude, iso_code, updated_date
        FROM cmis.electric_meter
        WHERE updated_date > (
            SELECT COALESCE(MAX(updated_date), '1900-01-01') FROM dwh.dim_electric_meter
        );

    c_meter_id INT;
    c_meter_type VARCHAR(50);
    c_voltage VARCHAR(50);
    c_status VARCHAR(50);
    c_location VARCHAR(255);
    c_latitude FLOAT8;
    c_longitude FLOAT8;
    c_iso_code VARCHAR(50);
    c_updated_date DATE;

    finished BOOLEAN := FALSE;

BEGIN
    -- Open the cursor
    OPEN meter_cursor;

    -- Fetch the first record
    FETCH meter_cursor INTO c_meter_id, c_meter_type, c_voltage, c_status, c_location, c_latitude, c_longitude, c_iso_code, c_updated_date;

    -- Check if the first record is null
    IF c_meter_id IS NULL THEN
        finished := TRUE;
    END IF;

    -- Loop through all records in cmis.electric_meter
    WHILE NOT finished LOOP
        -- Check if the meter exists in the dim_electric_meter table
        IF EXISTS (SELECT 1 FROM dwh.dim_electric_meter WHERE meter_id = c_meter_id) THEN
            -- Update the existing record with the latest data
            UPDATE dwh.dim_electric_meter
            SET meter_type = c_meter_type,
                voltage = c_voltage,
                status = c_status,
                location = c_location,
                latitude = c_latitude,
                longitude = c_longitude,
                iso_code = c_iso_code,
                updated_date = CURRENT_DATE
            WHERE meter_id = c_meter_id;
        ELSE
            -- Insert the new meter into dim_electric_meter
            INSERT INTO dwh.dim_electric_meter (meter_id, meter_type, voltage, status, location, latitude, longitude, iso_code, updated_date)
            VALUES (c_meter_id, c_meter_type, c_voltage, c_status, c_location, c_latitude, c_longitude, c_iso_code, CURRENT_DATE);
        END IF;

        -- Fetch the next record
        FETCH meter_cursor INTO c_meter_id, c_meter_type, c_voltage, c_status, c_location, c_latitude, c_longitude, c_iso_code, c_updated_date;

        -- Check if the next record is null
        IF c_meter_id IS NULL THEN
            finished := TRUE;
        END IF;
    END LOOP;

    -- Close the cursor
    CLOSE meter_cursor;

    -- Commit the changes
    COMMIT;
END;
$$;