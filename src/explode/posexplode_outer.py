from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode, col
from comm import create_sales_df
import time
spark = SparkSession.builder.appName("posexplode_outer").getOrCreate()
df=create_sales_df(spark)
df.select("*",posexplode("products").alias("position","product")).show()
