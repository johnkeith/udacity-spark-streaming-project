import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

from kafka_server import TOPIC_NAME, BOOTSTRAP_SERVER

schema = StructType([
    StructField('call_date_time', TimestampType(), False),
    StructField('disposition', StringType(), False),
    StructField('original_crime_type_name', StringType(), False),
])

def run_spark_job(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .option("subscribe", TOPIC_NAME) \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 100) \
        .option("startingOffsets","earliest") \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table.select(
        "call_date_time",
        "disposition",
        "original_crime_type_name"
    )

    watermarked_distinct_table = distinct_table.withWatermark("call_date_time", "20 minutes")

    agg_df = watermarked_distinct_table.groupBy(
        psf.window("call_date_time", "20 minutes"),
        "original_crime_type_name"
    ).count().orderBy("count")

    query = agg_df.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df.join(radio_code_df, "disposition")

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
