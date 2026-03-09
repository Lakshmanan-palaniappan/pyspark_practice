from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("collect_list").getOrCreate()
df = create_sales_df(spark)
df.groupBy("city").agg(collect_list("product")).show(truncate=False)