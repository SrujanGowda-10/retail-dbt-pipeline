SELECT
    product_code,
    quantity
FROM {{ref('fact_sales')}}
WHERE quantity <= 0
