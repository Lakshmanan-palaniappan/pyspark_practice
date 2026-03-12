from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark=SparkSession.builder.appName("write_json").getOrCreate()
df=spark.read.json("../../users.json",multiLine=True)
df.write.json("./outputs/users_json",mode="overwrite")
rd=spark.read.json("./outputs/users_json",multiLine=True)
flat=rd.select(explode("users").alias("user"))
users_df=flat.select("user.*")
users_df.show(truncate=False)