from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df_creation").getOrCreate()
data = [("aaa", 25, "IT"),("bbb", 30, "HR"),("ccc", 28, "Finance")]
columns = ["Name", "Age", "Department"]
df = spark.createDataFrame(data, columns)
df.show()
spark.stop()
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("").getOrCreate()
data = [[i, f"Name{i}", 20 + i % 10] for i in range(1,101)]
schema = "id INT, name STRING, age INT"
df = spark.createDataFrame(data, schema)
emp_final = df.filter((df.age > 21) & (df.age < 25))
emp_final.write.format('CSV').save('output/emp_age.csv')
emp_final.show()
emp_final.printSchema()