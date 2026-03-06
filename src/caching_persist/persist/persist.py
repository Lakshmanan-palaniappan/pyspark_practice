from pyspark.sql import SparkSession
from pyspark import StorageLevel
spark = SparkSession.builder.appName("persist").getOrCreate()
data = [("aaa", 25),("bbb", 30),("ccc", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.persist(StorageLevel.MEMORY_AND_DISK)
print(df.count())
spark.stop()