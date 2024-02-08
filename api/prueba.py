import grpc
import challenger_pb2 as ch
import challenger_pb2_grpc as api
from google.protobuf import empty_pb2

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Configuración de Spark
spark = SparkSession.builder.appName("StreamingFromCSV").getOrCreate()

# Esquema para los datos de Spark
schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("vault_id", IntegerType(), True)
])

# Función para procesar el lote con Spark
def processTheBatchQ1(batch):
    # Convertir el objeto de fecha y hora a un formato compatible con Spark
    formatted_states = [(datetime.fromtimestamp(state.date.seconds), state.serial_number, state.model, int(state.failure), state.vault_id) for state in batch.states]

    # Crear DataFrame de Spark
    df = spark.createDataFrame(formatted_states, schema=schema)

    # Aplicar operaciones de Spark
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

    return filteredData.collect()  # Devolver los datos procesados como lista

# Conexión con el servidor gRPC
with grpc.insecure_channel('challenge2024.debs.org:5023') as channel:
    stub = api.ChallengerStub(channel)

    # Paso 1 - Crear un nuevo Benchmark
    benchmarkconfiguration = ch.BenchmarkConfiguration(
        token='ljhcowvpnamgmyxfnsrqvhimyvcjdhzz',
        benchmark_name="this name shows_up_in_dashboard",
        benchmark_type="test",
        queries=[ch.Query.Q1, ch.Query.Q2]
    )
    benchmark = stub.createNewBenchmark(benchmarkconfiguration)

    stub.startBenchmark(benchmark)

    cnt_current = 0
    cnt_historic = 0
    cnt = 0

    # Procesar los lotes con Spark y enviar resultados al servidor gRPC
    batch = stub.nextBatch(benchmark)
    while batch:
        print('hola')
        processTheBatchQ1(batch)  # No es necesario almacenar el resultado en una variable
        print('chao')
        resultQ1 = ch.ResultQ1(
            benchmark_id=benchmark.id,
            batch_seq_id=batch.seq_id,
        )
        stub.resultQ1(resultQ1)

        if batch.last or cnt > 1_000:
            break

        cnt += 1
        batch = stub.nextBatch(benchmark)

    # Finaliza la medición del benchmark
    stub.endMeasurement(benchmark)

print("Finished")
