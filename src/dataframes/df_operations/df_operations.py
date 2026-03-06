from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
spark = SparkSession.builder.appName("df_operations").getOrCreate()
data = [("aaa", 25, "IT"),("bbb", 30, "HR"),("ccc", 28, "IT"),("ddd", 35, "Finance")]
columns = ["Name", "Age", "Department"]
df = spark.createDataFrame(data, columns)
print("Filtering Age > 26")
df.filter(df.Age > 26).show()
print("Selecting columns")
df.select("Name", "Department").show()
print("Average Age")
df.select(avg("Age")).show()
print("Group By Department")
df.groupBy("Department").count().show()
spark.stop()