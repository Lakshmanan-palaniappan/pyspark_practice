from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("row_number").getOrCreate()
df = create_sales_df(spark)
window = Window.partitionBy("city").orderBy("price")
df.withColumn("row_number", row_number().over(window)).show()