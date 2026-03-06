from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd_map").getOrCreate()
sc = spark.sparkContext
numbers = [1, 2, 3, 4, 5]
rdd = sc.parallelize(numbers)
squared_numbers = rdd.map(lambda x: x * x)
print("Squared values:")
print(squared_numbers.collect())