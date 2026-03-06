from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df_creation").getOrCreate()
data = [("aaa", 25, "IT"),("bbb", 30, "HR"),("ccc", 28, "Finance")]
columns = ["Name", "Age", "Department"]
df = spark.createDataFrame(data, columns)
df.show()
spark.stop()