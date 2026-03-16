from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder \
    .appName("managed_table") \
    .enableHiveSupport() \
    .getOrCreate()

sales_df = create_sales_df(spark, 20)

spark.sql("create database sales_db")
spark.sql("use sales_db")

sales_df.write \
    .mode("overwrite") \
    .saveAsTable("managed_sales")

spark.sql("select * from managed_sales").show()