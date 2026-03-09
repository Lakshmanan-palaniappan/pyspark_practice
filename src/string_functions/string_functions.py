from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.dataframes.df_creation.common_df import create_sales_df

spark = SparkSession.builder.appName("string_functions").getOrCreate()

df = create_sales_df(spark)

df.select(upper("product")).show()

df.select(trim("product")).show()

df.select(ltrim("product")).show()

df.select(rtrim("product")).show()

df.select(substring_index("city","a",1)).show()

df.select(substring("product",1,3)).show()

df.select(split("city","a")).show(truncate=False)

df.select(repeat("product",2)).show()

df.select(rpad("product",12,"*")).show()

df.select(lpad("product",12,"*")).show()

df.select(regexp_replace("city","a","@")).show()

df.select(lower("product")).show()

df.select(regexp_extract("salesperson","\\d+",0)).show()

df.select(length("product")).show()

df.select(instr("product","Lap")).show()

df.select(initcap("product")).show()