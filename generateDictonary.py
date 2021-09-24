from pyspark.sql import SQLContext
from utils.spark import createSparkSession

from utils.text import rdd_normalizer


if __name__ == "__main__":
    # Spark related
    spark = createSparkSession()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # Rdd from raw files as [(filename,(list_of_words))]
    raw_rdd = sc.wholeTextFiles("dataset/").map(rdd_normalizer)

    # Rdd with zipped distinct word as [(wordA,0),(wordB,1), ...]
    dict_rdd = (
        raw_rdd.map(lambda row: row[1])
        .flatMap(lambda row: row)
        .distinct()
        .zipWithIndex()
    )

    # Dataframe from dict RDD as [(word, wordId)]
    dict_df = sqlContext.createDataFrame(dict_rdd, ["word", "wordId"])

    # Save Parquet
    dict_df.write.parquet("output/dictionnary/", mode="overwrite")
