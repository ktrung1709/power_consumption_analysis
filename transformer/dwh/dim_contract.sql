CREATE OR REPLACE PROCEDURE dwh.update_dim_contract()
	LANGUAGE plpgsql
AS $$
	
    DECLARE
        contract_cursor CURSOR FOR
            SELECT contract_id, date_created, status, updated_date
            FROM cmis.contract
            WHERE updated_date > (
                SELECT COALESCE(MAX(updated_date), '1900-01-01') FROM dwh.dim_contract
            );

        c_contract_id INT;
        c_date_created DATE;
        c_status VARCHAR(20);
        c_updated_date DATE;

        finished BOOLEAN := FALSE;

    BEGIN
        -- Open the cursor
        OPEN contract_cursor;

        -- Loop through all records in cmis.contract
        FETCH contract_cursor INTO c_contract_id, c_date_created, c_status, c_updated_date;
		
       	-- Declare the handler for NOT FOUND
        IF c_contract_id is null THEN
           finished = TRUE;
        END IF; 
       
        WHILE NOT finished LOOP
            
            -- Check if the contract exists in the dim_contract table
            IF EXISTS (SELECT 1 FROM dwh.dim_contract WHERE contract_id = c_contract_id) THEN
                -- Update the existing record with the latest data
                UPDATE dwh.dim_contract
                SET date_created = c_date_created,
                    status = c_status,
                    updated_date = c_updated_date
                WHERE contract_id = c_contract_id;
            ELSE
                -- Insert the new contract into dim_contract
                INSERT INTO dwh.dim_contract (contract_id, date_created, status, updated_date)
                VALUES (c_contract_id, c_date_created, c_status, c_updated_date);
            END IF;

            -- Fetch the next contract
            FETCH contract_cursor INTO c_contract_id, c_date_created, c_status, c_updated_date;
            IF c_contract_id is null THEN
                finished = TRUE;
            END IF; 
        END LOOP;

        -- Close the cursor
        CLOSE contract_cursor;

        -- Commit the changes
        COMMIT;
    END;

$$
;