from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("collect_set").getOrCreate()
df = create_sales_df(spark)
df.groupBy("city").agg(collect_set("product")).show(truncate=False)