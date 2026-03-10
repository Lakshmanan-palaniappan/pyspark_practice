from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("rank").getOrCreate()
df = create_sales_df(spark)
window = Window.partitionBy("city").orderBy("price")
df.withColumn("rank", rank().over(window)).show()