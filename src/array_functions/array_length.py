from pyspark.sql import SparkSession
from pyspark.sql.functions import array, size
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("array_length").getOrCreate()
df = create_sales_df(spark)
df.select(size(array("product","city")).alias("length")).show()