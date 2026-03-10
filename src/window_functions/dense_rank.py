from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("dense_rank").getOrCreate()
df = create_sales_df(spark)
window = Window.partitionBy("city").orderBy("price")
df.withColumn("dense_rank", dense_rank().over(window)).show()