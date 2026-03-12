from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.appName("append_example").getOrCreate()
sales_data = [
    Row(city="Chennai", sales=12000),
    Row(city="Bangalore", sales=15000),
    Row(city="Hyderabad", sales=18000)
]
df_sales = spark.createDataFrame(sales_data)
df_sales.write.mode("overwrite").parquet("./outputs/sales_data/")
new_sales_data = [
    Row(city="Mumbai", sales=20000),
    Row(city="Delhi", sales=17000)
]
df_new_sales = spark.createDataFrame(new_sales_data)
df_new_sales.write.mode("append").parquet("./outputs/sales_data/")
df_appended = spark.read.parquet("./outputs/sales_data/")
df_appended.show()
