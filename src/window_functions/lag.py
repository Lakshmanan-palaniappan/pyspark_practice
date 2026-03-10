from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("lag").getOrCreate()
df = create_sales_df(spark)
window = Window.orderBy("price")
df.withColumn("previous_price", lag("price").over(window)).show()