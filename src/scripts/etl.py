import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definir a base do Glue Catalog
database_name = "ecommerce-database"

# Ler tabelas do Glue Catalog
customers = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, table_name="customers"
)
products = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, table_name="products"
)
orders = glueContext.create_dynamic_frame.from_catalog(
    database=database_name, table_name="orders"
)

# Transformações: Exemplo de join entre customers e orders
orders_with_customers = Join.apply(
    orders, customers, "customer_id", "customer_id"
)

# Exemplo de transformação: Adicionar uma coluna com valor calculado
from pyspark.sql.functions import col, lit

orders_with_customers_df = orders_with_customers.toDF()
orders_with_customers_df = orders_with_customers_df.withColumn(
    "total_order_value",
    col("order_item_quantity") * col("order_item_price"),
)

# Converter de volta para DynamicFrame
orders_with_customers = glueContext.create_dynamic_frame.from_dataframe(
    orders_with_customers_df, glueContext
)

# Carregar os dados processados para outro bucket S3
output_path = "s3://olist-processed-data/orders_with_customers/"
glueContext.write_dynamic_frame.from_options(
    frame=orders_with_customers,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
)

# Finalizar o job
job.commit()