from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType,StructField,StringType

@udf(returnType=StringType())
def combine_name(s1,s2):
    return s1+" "+s2
data=[
    \
    (1,("lakshmanan","palaniappan"),"Dev"), \
    (2,("arun","singh"),"Tester"),\
    ]
name_schema=StructType(
    [
        StructField("firstName",StringType()),
        StructField("lastName",StringType())

    ]
)
schema=StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",name_schema),
        StructField("position",StringType())
    ]
)
spark=SparkSession.builder.appName("udf").getOrCreate()
df=spark.createDataFrame(data,schema)
fullname_df=df.withColumn("fullname",combine_name(df.name.firstname,col("name.lastName")).alias("fullname"))
fullname_df.show()