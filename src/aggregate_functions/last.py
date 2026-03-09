from pyspark.sql import SparkSession
from pyspark.sql.functions import last
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("last").getOrCreate()
df = create_sales_df(spark)
df.groupBy("city").agg(last("product")).show()