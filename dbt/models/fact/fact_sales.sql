{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}


WITH retail_cleaned_stage AS (
    SELECT * FROM {{ref('retail_cleaned_stage')}}
)


SELECT 
    invoice_no,
    stock_code as product_code,
    quantity AS src_quantity,
    adjusted_quantity AS quantity,
    unit_price,
    customer_id,
    invoice_date,
    CASE
        WHEN transaction_type = 'sale' THEN 'sold'
        WHEN transaction_type = 'return' THEN 'cancelled'
    END AS transaction_type
FROM retail_cleaned_stage

{% if is_incremental() %}
    WHERE invoice_date > (SELECT COALESCE(MAX(invoice_date), '1900-01-01') FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_no,customer_id,product_code ORDER BY invoice_date DESC) = 1
