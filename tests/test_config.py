import unittest
from pyspark.sql import SparkSession
class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession
            .builder
            .master("local[1]")
            .appName("pyspark-unit-test")
            .getOrCreate()
        )
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()