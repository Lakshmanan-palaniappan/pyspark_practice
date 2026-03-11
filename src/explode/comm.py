import random
def create_sales_df(spark, rows=100):
    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard",None]
    cities = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad"]
    salespersons = ["e1", "e2", "e3", "e4", "e5"]
    customers = ["c" + str(i) for i in range(1, 21)]
    data = []
    for i in range(1, rows + 1):
        city = random.choice(cities)
        salesperson = random.choice(salespersons)
        customer = random.choice(customers)
        purchased_products = random.sample(products, random.randint(2, 4))
        quantity = random.randint(1, 10)
        price = round(random.uniform(100, 40000), 2)
        data.append((
            i, city, salesperson, customer, purchased_products, quantity, price
        ))
    df = spark.createDataFrame(
        data,
        ["sale_id", "city", "salesperson", "customer", "products", "quantity", "price"]
    )
    return df
