from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("write_parquet").getOrCreate()
df=spark.read.parquet("../../users.parquet")
df.write.parquet("./outputs/users_parquet",mode="overwrite")
rd=spark.read.parquet("./outputs/users_parquet")
rd.show()