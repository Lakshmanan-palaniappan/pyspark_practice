from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_remove
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("array_remove").getOrCreate()
df = create_sales_df(spark)
df.select(array_remove(array("product","city"),"Laptop")).show()