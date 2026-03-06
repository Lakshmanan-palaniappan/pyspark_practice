from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_creation").getOrCreate()
sc = spark.sparkContext
data = [10, 20, 30, 40, 50]
rdd = sc.parallelize(data)
print("RDD Elements:", rdd.collect())