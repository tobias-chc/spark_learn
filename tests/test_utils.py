from unittest import TestCase
from pyspark.sql import SparkSession
from hello_spark.lib.utils import load_csv, count_by_country


class utilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                .master("local[3]") \
                .appName("HelloSparkTest") \
                .getOrCreate()


    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_load_csv(self):
        sample_df = load_csv(self.spark, "hello_spark/data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Record count should be 9")

    def test_count_by_country(self):
        sample_df = load_csv(self.spark, "hello_spark/data/sample.csv")
        count_dict = dict(count_by_country(sample_df).collect())
        self.assertEqual(count_dict["United States"], 4, "Count for United States should be 4")
        self.assertEqual(count_dict["Canada"], 2, "Count for Canada should be 2")
        self.assertEqual(count_dict["United Kingdom"], 1, "Count United Kingdom for should be 1")


