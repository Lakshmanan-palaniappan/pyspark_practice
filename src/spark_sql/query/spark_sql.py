from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("py_sql").getOrCreate()
data = [("aaa", 50000),("bbb", 60000),("ccc", 45000)]
columns = ["Name", "Salary"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("employees")
result = spark.sql("SELECT * FROM employees WHERE Salary > 50000")
result.show()
spark.stop()