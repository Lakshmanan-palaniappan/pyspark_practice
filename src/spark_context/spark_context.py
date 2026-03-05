#Using Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test_session").getOrCreate()
sc = spark.sparkContext
print(sc)

#Using Spark Context(Old)
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf().setAppName("test_session_2")
sc = SparkContext(conf=conf)
print(sc)
