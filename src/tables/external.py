from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder \
    .appName("external_table") \
    .enableHiveSupport() \
    .getOrCreate()

sales_df = create_sales_df(spark, 20)

spark.sql("create database if not exists sales_db")
spark.sql("use sales_db")

data_path = "file:///tmp/external_sales_data"

sales_df.write \
    .mode("overwrite") \
    .parquet(data_path)

spark.sql(f"""
create table if not exists external_sales
using parquet
location '{data_path}'
""")

spark.sql("select * from external_sales").show()