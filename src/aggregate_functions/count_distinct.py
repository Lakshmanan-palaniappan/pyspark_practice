from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("countDistinct").getOrCreate()
df = create_sales_df(spark)
df.select(countDistinct("product")).show()