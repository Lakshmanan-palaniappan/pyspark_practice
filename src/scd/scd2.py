from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder.appName("scd_type_2").getOrCreate()

existing_sales = create_sales_df(spark, 10) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("is_current", lit(True))

incoming_sales = create_sales_df(spark, 5)

existing = existing_sales.alias("existing")
incoming = incoming_sales.alias("incoming")

changed_sales = existing.join(
    incoming,
    col("existing.sale_id") == col("incoming.sale_id")
).filter(col("existing.price") != col("incoming.price"))

expired_sales = changed_sales.select(
    col("existing.sale_id"),
    col("existing.product"),
    col("existing.city"),
    col("existing.salesperson"),
    col("existing.quantity"),
    col("existing.price"),
    col("existing.start_date"),
    current_date().alias("end_date"),
    lit(False).alias("is_current")
)

new_sales = incoming_sales \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("is_current", lit(True))

final_sales = existing_sales.union(expired_sales).union(new_sales)

final_sales.show()