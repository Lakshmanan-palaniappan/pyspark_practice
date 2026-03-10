import random

def create_join_dfs(spark, rows=100):

    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
    cities = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad"]
    salespersons = ["e1", "e2", "e3", "e4", "e5"]

    sales_data = []
    employee_data = []

    for i in range(1, rows + 1):

        sales_data.append((
            i,
            random.choice(products),
            random.randint(1,10),
            round(random.uniform(100,40000),2),
            random.choice(salespersons)
        ))

        employee_data.append((
            random.choice(salespersons),
            random.choice(cities)
        ))

    sales_df = spark.createDataFrame(
        sales_data,
        ["sale_id","product","quantity","price","salesperson"]
    )

    employee_df = spark.createDataFrame(
        employee_data,
        ["salesperson","city"]
    )

    return sales_df, employee_df