from pyspark.sql import SparkSession
from pyspark.sql.functions import pow
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("power").getOrCreate()
df = create_sales_df(spark)
df.select(pow("quantity",2)).show()