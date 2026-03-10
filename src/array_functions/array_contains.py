from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("array_contains").getOrCreate()
df = create_sales_df(spark)
df.select(array_contains(array("product","city"),"Laptop")).show()