from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("lead").getOrCreate()
df = create_sales_df(spark)
window = Window.orderBy("price")
df.withColumn("next_price", lead("price").over(window)).show()