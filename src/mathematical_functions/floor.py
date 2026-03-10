from pyspark.sql import SparkSession
from pyspark.sql.functions import floor
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("floor").getOrCreate()
df = create_sales_df(spark)
df.select(floor("price")).show()