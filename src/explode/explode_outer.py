from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from comm import create_sales_df
import time
spark=SparkSession.builder.appName("explode_outer").getOrCreate()
df=create_sales_df(spark)
outer_explode=df.withColumn("product",explode_outer(col=col("products")))
outer_explode.filter(col("product").isNull()).show()