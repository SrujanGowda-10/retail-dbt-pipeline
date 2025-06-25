WITH retail_claned_src AS (
    SELECT * FROM {{source('bq_source', 'retail_cleaned')}}
)

SELECT * FROM retail_claned_src
