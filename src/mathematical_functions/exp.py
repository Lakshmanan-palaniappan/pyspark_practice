from pyspark.sql import SparkSession
from pyspark.sql.functions import exp
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("exp").getOrCreate()
df = create_sales_df(spark)
df.select(exp("quantity")).show()