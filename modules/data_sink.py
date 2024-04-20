from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4j


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master('local[3]') \
        .appName('DataSinkDemo') \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format('parquet') \
        .load('data/flight*.parquet')

    logger.info(f"Number of partitions before: {flightTimeParquetDF.rdd.getNumPartitions()}")

    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info(f"Number of partitions after: {partitionedDF.rdd.getNumPartitions()}")
    partitionedDF.groupBy(spark_partition_id()).count().show()

    flightTimeParquetDF.write \
        .format('json') \
        .mode('overwrite') \
        .option('path', 'data/json/') \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    partitionedDF.write \
        .format('json') \
        .mode('overwrite') \
        .option('path', 'data/avro/') \
        .save()
