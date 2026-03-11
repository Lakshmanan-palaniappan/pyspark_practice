from pyspark.sql.functions import *
from src.dataframes.df_creation.common_df import create_sales_df
from test.test_config import SparkTestCase


class TestStringFunctions(SparkTestCase):

    def test_upper(self):

        df = create_sales_df(self.spark)

        result = df.select(upper("product"))

        self.assertTrue(result.count() > 0)

    def test_lower(self):

        df = create_sales_df(self.spark)

        result = df.select(lower("product"))

        self.assertTrue(result.count() > 0)

    def test_length(self):

        df = create_sales_df(self.spark)

        result = df.select(length("product"))

        self.assertTrue(result.count() > 0)