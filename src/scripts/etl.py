import sys
import logging
import unicodedata
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, count, sum, when, datediff, regexp_replace, lower, trim, lit, year, month, dayofmonth, date_format, udf, to_timestamp
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def remove_accents(input_str):
    if input_str is None:
        return None

    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])


unaccent = udf(remove_accents)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Initializing ETL job...")

# Initialize Glue and Spark Contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Job {args['JOB_NAME']} started.")

# Configuration
database_name = "olist-database"
postgresql_connection_options = {
    "url": "jdbc:postgresql://olist-rds-instance.c9qkuecgmgqu.us-east-1.rds.amazonaws.com/postgres",
    "user": "flyingsharks",
    "password": "I6AaxfUSY2Bs",
    "driver": "org.postgresql.Driver"
}

logger.info("Reading data from Glue Catalog...")

# Load data from Glue Catalog
customers_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="customers").toDF()
geolocation_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="geolocation").toDF()
order_items_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="order_items").toDF()
order_payments_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="order_payments").toDF()
order_reviews_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="order_reviews").toDF()
orders_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="orders").toDF()
products_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="products").toDF()
sellers_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="sellers").toDF()
category_translation_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="product_category_name_translation").toDF()

logger.info("Data loaded successfully.")
logger.info("Starting data cleaning...")

# Data Cleaning

# Customers: Remove null IDs or ZIP codes
customers_df = customers_df \
    .withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix.int").cast("int")) \
    .dropna(subset=["customer_id", "customer_zip_code_prefix"])

# Geolocation: Normalize city names and remove invalid coordinates
geolocation_df = geolocation_df \
    .withColumn("geolocation_zip_code_prefix", col("geolocation_zip_code_prefix.int").cast("int")) \
    .withColumn("geolocation_lat", col("geolocation_lat.double").cast("double")) \
    .withColumn("geolocation_lng", col("geolocation_lng.double").cast("double")) \
    .dropna(subset=["geolocation_lat", "geolocation_lng"]) \
    .filter((col("geolocation_lat") > -90) & (col("geolocation_lat") < 90)) \
    .filter((col("geolocation_lng") > -180) & (col("geolocation_lng") < 180)) \
    .withColumn("geolocation_city", lower(trim(unaccent(col("geolocation_city"))))) \
    .withColumn("geolocation_city", regexp_replace(col("geolocation_city"), r"[^a-zA-Z0-9\s]", ""))

# Order Items: Remove invalid prices or freight values
order_items_df = order_items_df \
    .withColumn("order_item_id", col("order_item_id.int").cast("int")) \
    .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("price", col("price.double").cast("double")) \
    .withColumn("freight_value", col("freight_value.double").cast("double")) \
    .dropna(subset=["price", "freight_value"]) \
    .filter((col("price") >= 0) & (col("freight_value") >= 0))

# Order Payments: Remove null or invalid payment values
order_payments_df = order_payments_df \
    .withColumn("payment_sequential", col("payment_sequential.int").cast("int")) \
    .withColumn("payment_installments", col("payment_installments.int").cast("int")) \
    .withColumn("payment_value", col("payment_value.double").cast("double")) \
    .dropna(subset=["payment_value"]) \
    .filter(col("payment_value") >= 0)

# Order Reviews: Normalize review messages and titles
order_reviews_df = order_reviews_df \
    .withColumn("review_score", col("review_score.int").cast("int")) \
    .withColumn("review_creation_date", to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna(subset=["review_score"]) \
    .withColumn("review_comment_message", lower(trim(unaccent(col("review_comment_message"))))) \
    .withColumn("review_comment_message", regexp_replace(col("review_comment_message"), r"[^a-zA-Z0-9\s]", "")) \
    .withColumn("review_comment_title", lower(trim(unaccent(col("review_comment_title"))))) \
    .withColumn("review_comment_title", regexp_replace(col("review_comment_title"), r"[^a-zA-Z0-9\s]", ""))

# Orders: Remove invalid dates or null order IDs
orders_df = orders_df \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna(subset=["order_id", "order_purchase_timestamp", "order_delivered_customer_date"]) \
    .filter(col("order_delivered_customer_date") >= col("order_purchase_timestamp"))

# Products: Remove invalid or null dimensions
products_df = products_df \
    .withColumn("product_name_length", col("product_name_length.int").cast("int")) \
    .withColumn("product_description_length", col("product_description_length.int").cast("int")) \
    .withColumn("product_photos_qty", col("product_photos_qty.int").cast("int")) \
    .withColumn("product_weight_g", col("product_weight_g.double").cast("double")) \
    .withColumn("product_length_cm", col("product_length_cm.double").cast("double")) \
    .withColumn("product_height_cm", col("product_height_cm.double").cast("double")) \
    .withColumn("product_width_cm", col("product_width_cm.double").cast("double")) \
    .dropna(subset=["product_id", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]) \
    .filter((col("product_weight_g") >= 0) & (col("product_length_cm") > 0) & (col("product_height_cm") > 0) & (col("product_width_cm") > 0))

# Sellers: Normalize city names and remove special characters
sellers_df = sellers_df \
    .withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix.int").cast("int")) \
    .dropna(subset=["seller_id", "seller_zip_code_prefix"]) \
    .withColumn("seller_city", lower(trim(unaccent(col("seller_city"))))) \
    .withColumn("seller_city", regexp_replace(col("seller_city"), r"[^a-zA-Z0-9\s]", ""))

logger.info("Data cleaning completed.")

tables = {
    "customers": customers_df,
    "orders": orders_df,
    "products": products_df,
    "sellers": sellers_df,
    "geolocation": geolocation_df,
    "order_items": order_items_df,
    "order_payments": order_payments_df,
    "order_reviews": order_reviews_df,
    "product_category_name_translation": category_translation_df
}

logger.info("Writing data to PostgreSQL...")

for table_name, df in tables.items():
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glueContext, table_name),
        connection_type="postgresql",
        connection_options={**postgresql_connection_options, "dbtable": table_name},
        transformation_ctx=f"postgresql_write_{table_name}"
    )

logger.info("Data written to PostgreSQL successfully.")

# Commit job
job.commit()
