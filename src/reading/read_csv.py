from pyspark.sql import SparkSession
import pathlib
spark=SparkSession.builder.appName("read_csv").getOrCreate()
df=spark.read.csv("../../sales.csv",header=True)
df.printSchema()