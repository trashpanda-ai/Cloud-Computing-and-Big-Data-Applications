import os
import logging
from datetime import datetime
import grpc
from google.protobuf import empty_pb2
import challenger_pb2 as ch
import challenger_pb2_grpc as api

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()


 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()