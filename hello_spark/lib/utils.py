import configparser
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

def get_spark_app_conf(spark_conf_path: str = "spark.conf") -> SparkConf:
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.optionxform = str  # Preserve original case of keys
    config.read(spark_conf_path)

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)

    return spark_conf


def load_csv(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
