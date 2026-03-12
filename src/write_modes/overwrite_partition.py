from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("write_parquet").getOrCreate()
df=spark.read.parquet("../../users.parquet")
outlet_sales=df.filter(col("salary")>25000)
outlet_sales=outlet_sales.repartition(4)
outlet_sales.write.mode("overwrite").option("partitionOverwriteMode","dynamic").parquet("./outputs/sal_25000/")
outlet_sales.show()