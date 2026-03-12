from pyspark.sql import SparkSession
from delta.tables import DeltaTable
spark = SparkSession.builder \
    .appName("upsert") \
    .config("spark.jars.packages","io.delta:delta-spark_2.13:4.1.0") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
data = [("Chennai",1000),("Mumbai",2000)]
df = spark.createDataFrame(data,["city","sales"])
df.write.format("delta").mode("overwrite").save("./city_sales")
new_data = [("Mumbai",2500),("Delhi",3000)]
new_df = spark.createDataFrame(new_data,["city","sales"])
delta_table = DeltaTable.forPath(spark,"./city_sales")
delta_table.alias("t").merge(
    new_df.alias("s"),
    "t.city = s.city"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
spark.read.format("delta").load("./city_sales").show()