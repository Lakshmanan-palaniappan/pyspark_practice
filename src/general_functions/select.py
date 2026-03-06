from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("select").getOrCreate()
data = [("aaa",25),("bbb",30)]
df = spark.createDataFrame(data,["Name","Age"])
df.select("Name").show()
spark.stop()