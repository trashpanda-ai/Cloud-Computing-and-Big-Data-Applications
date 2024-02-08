import os
import logging
from datetime import datetime
import grpc
from google.protobuf import empty_pb2
import challenger_pb2 as ch
import challenger_pb2_grpc as api

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, split, sum, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, ArrayType

op = [('grpc.max_send_message_length', 10 * 1024 * 1024),
      ('grpc.max_receive_message_length', 100 * 1024 * 1024)]
spark = SparkSession.builder.appName("JsonParsingExample").getOrCreate()

schema = StructType([
    StructField("Drive State", StructType([
        StructField("Timestamp", StructType([
            StructField("seconds", IntegerType())
        ])),
        StructField("Serial Number", StringType()),
        StructField("Model", StringType()),
        StructField("Failure", BooleanType()),
        StructField("Vault ID", IntegerType()),
        StructField("Readings", ArrayType(IntegerType()))
    ]))
])

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "challenge2024.debs.org") \
    .option("port", 5023) \
    .load()

streamingData = lines \
    .select(from_json(lines.value, schema).alias("data")) \
    .select("data.*")


windowedData = streamingData \
    .withWatermark("Timestamp", "31 days") \
    .groupBy(
        window(streamingData["Timestamp"]["seconds"], "30 days", "1 day"),
        streamingData["Vault ID"],
        streamingData["Model"]
    ) \
    .agg(sum("Failure").alias("total_failures"))

selectedData = windowedData.select("window.start", "Vault ID", "total_failures")

filteredData = selectedData.filter(selectedData.total_failures > 0)

query = filteredData \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("filteredData") \
    .option("numRows", 50) \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
query.stop()
