CREATE OR REPLACE PROCEDURE dwh.update_dim_customer()
	LANGUAGE plpgsql
AS $$
	
    DECLARE
        customer_cursor CURSOR FOR
            SELECT customer_id, customer_name, phone, email, address, customer_type, updated_date
            FROM cmis.customer
            WHERE updated_date > (
                SELECT COALESCE(MAX(updated_date), '1900-01-01') FROM dwh.dim_customer
            );

        c_customer_id INT;
        c_customer_name VARCHAR(255);
        c_phone VARCHAR(255);
        c_email VARCHAR(255);
        c_address VARCHAR(255);
        c_customer_type VARCHAR(20);
        c_updated_date DATE;

        finished BOOLEAN := FALSE;

    BEGIN
        -- Open the cursor
        OPEN customer_cursor;

        -- Loop through all records in cmis.customer
        FETCH customer_cursor INTO c_customer_id, c_customer_name, c_phone, c_email, c_address, c_customer_type, c_updated_date;
		
       	-- Declare the handler for NOT FOUND
        IF c_customer_id is null THEN
           finished = TRUE;
        END IF; 
       
        WHILE NOT finished LOOP
            
            -- Check if the customer exists in the dim_customer table
            IF EXISTS (SELECT 1 FROM dwh.dim_customer WHERE customer_id = c_customer_id) THEN
                -- Update the existing record with the latest data
                UPDATE dwh.dim_customer
                SET customer_name = c_customer_name,
                    phone = c_phone,
                    email = c_email,
                    address = c_address,
                    customer_type = c_customer_type,
                    updated_date = c_updated_date
                WHERE customer_id = c_customer_id;
            ELSE
                -- Insert the new customer into dim_customer
                INSERT INTO dwh.dim_customer (customer_id, customer_name, phone, email, address, customer_type, updated_date)
                VALUES (c_customer_id, c_customer_name, c_phone, c_email, c_address, c_customer_type, c_updated_date);
            END IF;

            -- Fetch the next customer
            FETCH customer_cursor INTO c_customer_id, c_customer_name, c_phone, c_email, c_address, c_customer_type, c_updated_date;
            IF c_customer_id is null THEN
                finished = TRUE;
            END IF; 
        END LOOP;

        -- Close the cursor
        CLOSE customer_cursor;

        -- Commit the changes
        COMMIT;
    END;

$$
;
