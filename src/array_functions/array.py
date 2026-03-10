from pyspark.sql import SparkSession
from pyspark.sql.functions import array
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("array").getOrCreate()
df = create_sales_df(spark)
df.select(array("product","city")).show(truncate=False)