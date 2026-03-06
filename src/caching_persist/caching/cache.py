from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cache").getOrCreate()
data = [("aaa", 25),("bbb", 30),("ccc", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.cache()
df.show()
spark.stop()