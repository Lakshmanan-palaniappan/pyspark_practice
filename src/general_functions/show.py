from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("show").getOrCreate()
data = [("aaa",25),("bbb",30)]
df = spark.createDataFrame(data,["Name","Age"])
df.show()
spark.stop()