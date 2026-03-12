from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

spark=SparkSession.builder.appName("write_json").getOrCreate()
df=spark.read.csv("../../sales.csv",header=True)
outlet_sales=df.filter(col("Item_Outlet_Sales").cast(DoubleType())>3000)
outlet_sales = outlet_sales.repartition(4)
#outlet_sales.write.csv("./outputs/outlet_sales_3000/", header=True)
outlet_sales=df.filter(col("Item_Outlet_Sales").cast(DoubleType())>2500)
outlet_sales = outlet_sales.repartition(4)
outlet_sales.write.csv("./outputs/outlet_sales_3000/",mode="overwrite", header=True)
outlet_sales.show()
