SELECT
    product_code,
    unit_price
FROM {{ref('fact_sales')}}
WHERE unit_price <= 0
