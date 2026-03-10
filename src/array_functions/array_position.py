from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_position
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("array_position").getOrCreate()
df = create_sales_df(spark)
df.select(array_position(array("product","city"),"Laptop")).show()