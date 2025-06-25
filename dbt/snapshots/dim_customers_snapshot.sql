{% snapshot dim_customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['residing_country']
  )
}}


SELECT 
    customer_id,
    country AS residing_country
FROM {{ ref('retail_cleaned_stage') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY invoice_date DESC)=1

{% endsnapshot %}
