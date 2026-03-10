from pyspark.sql import SparkSession
from pyspark.sql.functions import sqrt
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("sqrt").getOrCreate()
df = create_sales_df(spark)
df.select(sqrt("price")).show()