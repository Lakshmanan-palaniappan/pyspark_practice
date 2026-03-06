from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("collect").getOrCreate()
data = [("aaa",25),("bbb",30)]
df = spark.createDataFrame(data,["Name","Age"])
rows = df.collect()
for r in rows:
    print(r)
spark.stop()