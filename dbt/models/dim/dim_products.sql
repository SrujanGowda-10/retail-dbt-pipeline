{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unique_key='product_code'
    )
}}


WITH retail_cleaned_stage AS (
    SELECT * FROM {{ ref('retail_cleaned_stage') }}
)

SELECT DISTINCT
    stock_code AS product_code,
    TRIM(description) as description
FROM retail_cleaned_stage

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} t
    WHERE t.product_code = retail_cleaned_stage.stock_code
)
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY product_code ORDER BY description) = 1