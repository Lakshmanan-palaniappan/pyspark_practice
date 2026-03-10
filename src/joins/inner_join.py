from pyspark.sql import SparkSession
from src.dataframes.df_creation.join_dfs import create_join_dfs
spark = SparkSession.builder.appName("inner_join").getOrCreate()
sales_df, employee_df = create_join_dfs(spark)
s = sales_df.alias("s")
e = employee_df.alias("e")
s.join(
    e,
    s.salesperson == e.salesperson,
    "inner"
).select(
    "s.sale_id",
    "s.product",
    "e.city"
).show()