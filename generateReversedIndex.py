from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils.spark import createSparkSession

from utils.text import rdd_normalizer


if __name__ == "__main__":

    # Spark related
    spark = createSparkSession()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # Dictonary index [("word", "wordId")]
    dict_df = spark.read.parquet("output/dictionnary/")

    # DF from raw files resulting in [(filename, word)]
    raw_rdd = (
        sc.wholeTextFiles("dataset/")
        .map(rdd_normalizer)
        .flatMap(lambda r: [(int(r[0]), i) for i in r[1]])
    )
    raw_df = sqlContext.createDataFrame(raw_rdd, ["filename", "word"])

    # Join using broadcast on the dictonary, to increase performance
    mapped_df = raw_df.join(
        F.broadcast(dict_df), on=raw_df.word == dict_df.word, how="left"
    )

    # Aggregations to generate reversed index
    w = Window.partitionBy("wordId").orderBy("filename")
    df3 = mapped_df.withColumn(
        "ordered_value_lists", F.collect_list("filename").over(w)
    )
    df4 = (
        df3.groupBy("wordId")
        .agg(F.max("ordered_value_lists").alias("values"))
        .orderBy("wordId")
    )

    # Optional cache to speedup final outputs and show
    df4.cache()

    # Saving result as parquet
    df4.write.parquet("output/reversed_index/", mode="overwrite")
    print("File reversed_index saved at output/reversed_index/")

    df4.show()
    print("END")
