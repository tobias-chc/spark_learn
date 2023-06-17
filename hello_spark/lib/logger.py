from pyspark.sql import SparkSession


class Log4j:
    def __init__(self, spark: SparkSession, root_class: str = "tobias.sparklearning.spark.examples"):
        log4j = spark._jvm.org.apache.log4j
        self.conf = spark.sparkContext.getConf()
        app_name = self.conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(f"{root_class}.{app_name}")
        # print(conf.get('spark.driver.extraJavaOptions'))
        # print(conf.getAll())
    
    def get_conf(self):
        self.info(self.conf.toDebugString())
    
    def warn(self, message: str):
        self.logger.warn(message)

    def info(self, message: str):
        self.logger.info(message)

    def debug(self, message: str):
        self.logger.debug(message)

    def error(self, message: str):
        self.logger.error(message)
