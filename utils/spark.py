from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def createSparkSession() -> SparkSession:
    """Create and return the SparkSession."""
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setExecutorEnv("spark.executor.memory", "4g")
        .setExecutorEnv("spark.driver.memory", "2g")
        .setAppName("de_test_glofox")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark
