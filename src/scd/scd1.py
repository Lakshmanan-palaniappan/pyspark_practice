from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder.appName("scd_type_1").getOrCreate()

existing_sales = create_sales_df(spark, 10)
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
    coalesce(col("incoming.product"), col("existing.product")).alias("product"),
    coalesce(col("incoming.city"), col("existing.city")).alias("city"),
    coalesce(col("incoming.salesperson"), col("existing.salesperson")).alias("salesperson"),
    coalesce(col("incoming.quantity"), col("existing.quantity")).alias("quantity"),
    coalesce(col("incoming.price"), col("existing.price")).alias("price")
)
existing.show()
incoming.show()
updated_sales.show()