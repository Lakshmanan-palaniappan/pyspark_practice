from pyspark.sql import SparkSession
from pyspark.sql.functions import ceil
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("ceil").getOrCreate()
df = create_sales_df(spark)
df.select(ceil("price")).show()