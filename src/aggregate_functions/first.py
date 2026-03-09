from pyspark.sql import SparkSession
from pyspark.sql.functions import first
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("first").getOrCreate()
df = create_sales_df(spark)
df.groupBy("city").agg(first("product")).show()