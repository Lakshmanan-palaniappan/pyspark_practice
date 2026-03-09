from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("count").getOrCreate()
df = create_sales_df(spark)
df.select(count("sale_id")).show()