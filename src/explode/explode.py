from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from comm import create_sales_df
import time
spark=SparkSession.builder.appName("explode").getOrCreate()
df=create_sales_df(spark)
exploded_df=df.withColumn("product",explode(col("products")))
exploded_df.show()
time.sleep(120)