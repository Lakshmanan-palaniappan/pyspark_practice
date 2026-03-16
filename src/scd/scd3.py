from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder.appName("scd_type_3").getOrCreate()

existing_sales = create_sales_df(spark, 10) \
    .withColumn("previous_price", col("price"))

incoming_sales = create_sales_df(spark, 5)

existing = existing_sales.alias("existing")
incoming = incoming_sales.alias("incoming")

joined_sales = existing.join(
    incoming,
    col("existing.sale_id") == col("incoming.sale_id"),
    "left"
)

updated_sales = joined_sales.select(
    col("existing.sale_id"),
    col("existing.product"),
    col("existing.city"),
    col("existing.salesperson"),
    col("existing.quantity"),
    col("incoming.price").alias("price"),
    col("existing.price").alias("previous_price")
)

updated_sales.show()