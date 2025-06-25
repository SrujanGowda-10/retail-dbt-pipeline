# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--input_path")
parser.add_argument("--output_table")
parser.add_argument("--temp_bucket")
args = parser.parse_args()


spark = SparkSession.builder.appName('Retail ETL').getOrCreate()

#  Read the retail data
schema = StructType([
    StructField('InvoiceNo', StringType(), True),
    StructField('StockCode', StringType(), True),
    StructField('Description', StringType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('InvoiceDate', StringType(), True),
    StructField('UnitPrice', DoubleType(), True),
    StructField('CustomerID', IntegerType(), True),
    StructField('Country', StringType(), True)
])
retail_df = spark.read.schema(schema)\
        .option('Header', True)\
            .csv(args.input_path)


# 1. Renaming columns
retail_df = retail_df.select(
    col('InvoiceNo').alias('invoice_no'),
    col('StockCode').alias('stock_code'),
    col('Description').alias('description'),
    col('Quantity').alias('quantity'),
    col('InvoiceDate').alias('invoice_date'),
    col('UnitPrice').alias('unit_price'),
    col('CustomerID').alias('customer_id'),
    col('Country').alias('country')
)


# 2. Dropping the duplicates rows
retail_df = retail_df.dropDuplicates()


# 3. Dropping the rows where non-null columns having null
retail_df = retail_df.dropna(subset=['invoice_no','stock_code'])


#  4. Adding customer info check flag
retail_df = retail_df.withColumn(
    'is_customer_info_missing',
    when(col('customer_id').isNull(), 1).otherwise(0)
)


# 5. Adding a flag to identify product and non-product info
non_products_stock_code = ['C2', 'D', 'M', 'DOT', 'POST', 'CRUK', 'BANK CHARGES']

retail_df = retail_df.withColumn(
    'is_product',
    when(col('stock_code').isin(non_products_stock_code), 0).otherwise(1)
)


# 6. Cleansed DF
cleansed_df = retail_df.filter(
    (col('is_customer_info_missing')==0) & (col('is_product') == 1)
)


# 7. Adding transaction_type flag to identify returns and sales
cleansed_df = cleansed_df.withColumn(
    'transaction_type',
    when(col('invoice_no').startswith('C'), 'return').otherwise('sale')
)


# 8. Adjusting the -ve quantity for returned transaction
cleansed_df = cleansed_df.withColumn(
    'adjusted_quantity',
    when(col('transaction_type')=='return', -col('quantity')).otherwise(col('quantity'))
)


# 9. Cast date column to timestamp
cleansed_df = cleansed_df.withColumn(
    'invoice_date',
    to_timestamp('invoice_date', 'dd/MM/yyyy HH:mm')
)


# 10. Remove irrelevant rows
cleansed_df = cleansed_df.filter((col("quantity") != 0) & (col("unit_price").isNotNull()) & (col("unit_price") != 0))


# 11. Trim Whitespace
cleansed_df = cleansed_df.withColumn('invoice_no', trim(col('invoice_no')))\
                    .withColumn('stock_code', trim(col('stock_code')))\
                            .withColumn('description', trim(col('description')))


# 12. Remove irrelevant columns
cleansed_df = cleansed_df.drop('is_customer_info_missing','is_product')


# 13 Write to BQ table
cleansed_df.write\
    .format('bigquery')\
        .option('table', args.output_table)\
            .option('temporaryGcsBucket', args.temp_bucket)\
                .mode('overwrite')\
                    .save()
