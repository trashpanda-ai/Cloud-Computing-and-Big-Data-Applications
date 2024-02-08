import grpc
import challenger_pb2 as ch
import challenger_pb2_grpc as api
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Spark configuration
spark = SparkSession.builder.appName("StreamingFromCSV").getOrCreate()

# Schema for Spark data
schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("vault_id", IntegerType(), True)
])

# Function to process the batch with Spark
def processTheBatchQ1(batch):
    # Convert timestamp object to Spark compatible format
    formatted_states = [(datetime.fromtimestamp(state.date.seconds), state.serial_number, state.model, int(state.failure), state.vault_id) for state in batch.states]

    # Create Spark DataFrame
    df = spark.createDataFrame(formatted_states, schema=schema)

    # Apply Spark operations
    windowedData = df \
        .withWatermark("date", "31 days") \
        .groupBy(
            df.vault_id,
            window(df.date, "30 days", "1 day"),
            df.model
        ) \
        .agg(_sum("failure").alias("total_failures"))

    selectedData = windowedData.select("window.start", "vault_id", "total_failures")
    filteredData = selectedData.filter(selectedData.total_failures > 0)

    return filteredData.collect()  # Return processed data as list

# gRPC server connection
with grpc.insecure_channel('challenge2024.debs.org:5023') as channel:
    stub = api.ChallengerStub(channel)

    # Step 1 - Create a new Benchmark
    benchmarkconfiguration = ch.BenchmarkConfiguration(
        token='ljhcowvpnamgmyxfnsrqvhimyvcjdhzz',
        benchmark_name="this name shows_up_in_dashboard",
        benchmark_type="test",
        queries=[ch.Query.Q1, ch.Query.Q2]
    )
    benchmark = stub.createNewBenchmark(benchmarkconfiguration)

    stub.startBenchmark(benchmark)

    cnt = 0

    # Process batches with Spark and send results to gRPC server
    batch = stub.nextBatch(benchmark)
    while batch and cnt < 1000:
        print('Processing batch', cnt)
        processed_data = processTheBatchQ1(batch)  # Process the batch and store the result
        
        # Send processed data to gRPC server
        for row in processed_data:
            resultQ1 = ch.ResultQ1(
                benchmark_id=benchmark.id,
                batch_seq_id=batch.seq_id,
            )
            stub.resultQ1(resultQ1)

        if batch.last:
            break

        cnt += 1
        batch = stub.nextBatch(benchmark)

    # Finish benchmark measurement
    stub.endBenchmark(benchmark)

print("Finished")
