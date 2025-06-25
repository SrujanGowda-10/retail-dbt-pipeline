WITH customer_latest AS (
    SELECT * FROM {{ref('dim_customers_snapshot')}}
    WHERE dbt_valid_to IS NULL
)

SELECT DISTINCT
    customer_id,
    residing_country
FROM customer_latest
