--RUN ONLY ONCE Create sequences for surrogate keys
CREATE SEQUENCE IF NOT EXISTS product_seq START = 1;
CREATE SEQUENCE IF NOT EXISTS customer_seq START = 1;
CREATE SEQUENCE IF NOT EXISTS order_seq START = 1;
CREATE SEQUENCE IF NOT EXISTS date_seq START = 1;

-- RUN ONLY ONCE Create a reusable Parquet file format
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'PARQUET';


-- Creating the main procedure to load dimension and fact tables
USE SCHEMA RETAIL_DB.SALES_WH; 
CREATE OR REPLACE PROCEDURE RETAIL_DB.SALES_WH.load_dim_tables_and_fact_table()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
    folder_name STRING;
    full_stage_path STRING;
BEGIN

-- Get latest folder name from staged file list
    SELECT folder
    INTO folder_name
    FROM (
        SELECT SPLIT_PART(METADATA$FILENAME, '/', 1) AS folder
        FROM @RETAIL_DB.SALES_WH.RETAIL_SALES_STAGE
        WHERE METADATA$FILENAME ILIKE '%/retail_sales_data.parquet'
        ORDER BY folder DESC
        LIMIT 1
    );

    full_stage_path := '@RETAIL_DB.SALES_WH.RETAIL_SALES_STAGE/'|| folder_name ||'/retail_sales_data.parquet';

-- =============== DIM PRODUCT ===============
    sql_stmt := '
        MERGE INTO RETAIL_DB.SALES_WH.dim_product tgt
        USING (
            SELECT DISTINCT 
                $1:PRODUCTCODE AS product_id,
                $1:PRODUCTLINE AS product_line,
                $1:MSRP AS msrp
            FROM ' || full_stage_path || ' (FILE_FORMAT => parquet_format)
        ) src
        ON tgt.product_id = src.product_id
        WHEN NOT MATCHED THEN
            INSERT (
                product_key,
                product_id,
                product_line,
                msrp
            )
            VALUES (
                product_seq.NEXTVAL,
                src.product_id,
                src.product_line,
                src.msrp
            );';

    EXECUTE IMMEDIATE sql_stmt;

-- =============== DIM CUSTOMER ===============
    sql_stmt := '
        MERGE INTO RETAIL_DB.SALES_WH.dim_customer tgt
        USING (
            SELECT DISTINCT 
                $1:CUSTOMERNAME AS customer_name,
                $1:PHONE AS phone,
                $1:ADDRESSLINE1 AS addressline1,
                $1:ADDRESSLINE2 AS addressline2,
                $1:CITY AS city,
                $1:STATE AS state,
                $1:COUNTRY AS country,
                $1:POSTALCODE AS postalcode,
                $1:CONTACTFIRSTNAME AS contactfirstname,
                $1:CONTACTLASTNAME AS contactlastname
            FROM ' || full_stage_path || ' (FILE_FORMAT => parquet_format)
        ) src
        ON tgt.customer_name = src.customer_name AND tgt.phone = src.phone
        WHEN NOT MATCHED THEN
            INSERT (
                customer_key,
                customer_name,
                phone,
                address_line1,
                address_line2,
                city,
                state,
                country,
                postal_code,
                contact_first_name,
                contact_last_name
            )
            VALUES (
                customer_seq.NEXTVAL,
                src.customer_name,
                src.phone,
                src.addressline1,
                src.addressline2,
                src.city,
                src.state,
                src.country,
                src.postalcode,
                src.contactfirstname,
                src.contactlastname
            );';

    EXECUTE IMMEDIATE sql_stmt;

-- =============== DIM ORDER ===============
    sql_stmt := '
        MERGE INTO RETAIL_DB.SALES_WH.dim_order tgt
        USING (
            SELECT DISTINCT 
                $1:ORDERNUMBER AS order_id,
                $1:ORDERLINENUMBER AS line_number,
                $1:QUANTITYORDERED AS quantity,
                $1:SALES AS total_price,
                $1:STATUS AS order_status,
                TO_DATE($1:DATE) AS order_date,
                $1:DEALSIZE AS deal_size
            FROM ' || full_stage_path || ' (FILE_FORMAT => parquet_format)
        ) src
        ON tgt.order_id = src.order_id AND tgt.line_number = src.line_number
        WHEN NOT MATCHED THEN
            INSERT (
                order_key,
                order_id,
                line_number,
                quantity,
                total_price,
                order_status,
                order_date,
                deal_size
            )
            VALUES (
                order_seq.NEXTVAL,
                src.order_id,
                src.line_number,
                src.quantity,
                src.total_price,
                src.order_status,
                src.order_date,
                src.deal_size
            );';

    EXECUTE IMMEDIATE sql_stmt;

-- =============== DIM DATE ===============
    sql_stmt := '
        MERGE INTO RETAIL_DB.SALES_WH.dim_date tgt
        USING (
            SELECT DISTINCT 
                TO_DATE($1:DATE) AS order_date,
                DAY(TO_DATE($1:DATE))::INT AS date_val,
                MONTH(TO_DATE($1:DATE))::INT AS month_val,
                YEAR(TO_DATE($1:DATE))::INT AS year_val
            FROM ' || full_stage_path || ' (FILE_FORMAT => parquet_format)
        ) src
        ON tgt.order_date = src.order_date
        WHEN NOT MATCHED THEN
            INSERT (
                date_key,
                order_date,
                date_val,
                month_val,
                year_val
            )
            VALUES (
                date_seq.NEXTVAL,
                src.order_date,
                src.date_val,
                src.month_val,
                src.year_val
            );';

    EXECUTE IMMEDIATE sql_stmt;

-- =============== FACT SALES ===============
    sql_stmt := '
        MERGE INTO RETAIL_DB.SALES_WH.fact_sales tgt
        USING (
            SELECT
                o.order_key,
                p.product_key,
                c.customer_key,
                d.date_key,
                f.$1:QUANTITYORDERED AS quantity,
                f.$1:PRICEEACH AS price_each,
                f.$1:SALES AS total_price
            FROM ' || full_stage_path || ' (FILE_FORMAT => parquet_format) f
            JOIN dim_order o ON o.order_id = f.$1:ORDERNUMBER AND o.line_number = f.$1:ORDERLINENUMBER
            JOIN dim_product p ON p.product_id = f.$1:PRODUCTCODE
            JOIN dim_customer c ON c.customer_name = f.$1:CUSTOMERNAME AND c.phone = f.$1:PHONE
            JOIN dim_date d ON d.order_date = TO_DATE(f.$1:DATE)
        ) src
        ON tgt.order_key = src.order_key
            AND tgt.product_key = src.product_key
            AND tgt.customer_key = src.customer_key
            AND tgt.date_key = src.date_key
        WHEN NOT MATCHED THEN
            INSERT (
                order_key,
                product_key,
                customer_key,
                date_key,
                quantity,
                price_each,
                total_price
            )
            VALUES (
                order_key,
                product_key,
                customer_key,
                date_key,
                quantity,
                price_each,
                total_price
            );';

    EXECUTE IMMEDIATE sql_stmt;

RETURN '✅ All dimension and fact tables loaded successfully.';
END;
$$;
