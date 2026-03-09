from pyspark.sql import SparkSession
import random

def create_sales_df(spark, rows=100):

    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
    cities = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad"]
    salespersons = ["e1", "e2", "e3", "e4", "e5"]

    data = []

    for i in range(1, rows + 1):
        data.append((
            i,
            random.choice(products),
            random.choice(cities),
            random.choice(salespersons),
            random.randint(1, 10),
            round(random.uniform(100, 40000), 2)
        ))

    df = spark.createDataFrame(
        data,
        ["sale_id", "product", "city", "salesperson", "quantity", "price"]
    )

    return df