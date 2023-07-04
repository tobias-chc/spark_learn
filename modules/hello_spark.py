import sys
from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import get_spark_app_conf, load_csv, count_by_country

# SPARK_HOME="/Users/tobiaschavarria/Library/Caches/pypoetry/virtualenvs/spark-learn-iD0jGMx_-py3.11/lib/python3.11/site-packages/pyspark"

if __name__ == "__main__":
    conf = get_spark_app_conf(spark_conf_path="hello_spark.conf")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: 'main <filename>'")
        sys.exit(-1)

    logger.info("Starting Hello Spark!")
    # logger.get_conf()

    #Processing code
    survey_df = load_csv(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)

    logger.info(count_df.collect())

    logger.info("Stopping Hello Spark!")
    spark.stop()
