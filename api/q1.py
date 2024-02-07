from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import window, col, sum as _sum

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import StreamingKMeans

spark = SparkSession.builder.appName("StreamingFromCSV").getOrCreate()

schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("failure", IntegerType(), True),
    StructField("vault_id", IntegerType(), True),
    StructField("s1_read_error_rate", IntegerType(), True),
    StructField("s2_throughput_performance", IntegerType(), True),
    StructField("s3_spin_up_time", IntegerType(), True),
    StructField("s4_start_stop_count", IntegerType(), True),
    StructField("s5_reallocated_sector_count", IntegerType(), True),
    StructField("s7_seek_error_rate", IntegerType(), True),
    StructField("s8_seek_time_performance", IntegerType(), True),
    StructField("s9_power_on_hours", IntegerType(), True),
    StructField("s10_spin_retry_count", IntegerType(), True),
    StructField("s12_power_cycle_count", IntegerType(), True),
    StructField("s173_wear_leveling_count", IntegerType(), True),
    StructField("s174_unexpected_power_loss_count", IntegerType(), True),
    StructField("s183_sata_downshift_count", IntegerType(), True),
    StructField("s187_reported_uncorrectable_errors", IntegerType(), True),
    StructField("s188_command_timeout", IntegerType(), True),
    StructField("s189_high_fly_writes", IntegerType(), True),
    StructField("s190_airflow_temperature_cel", IntegerType(), True),
    StructField("s191_g_sense_error_rate", IntegerType(), True),
    StructField("s192_power_off_retract_count", IntegerType(), True),
    StructField("s193_load_unload_cycle_count", IntegerType(), True),
    StructField("s194_temperature_celsius", IntegerType(), True),
    StructField("s195_hardware_ecc_recovered", IntegerType(), True),
    StructField("s196_reallocated_event_count", IntegerType(), True),
    StructField("s197_current_pending_sector", IntegerType(), True),
    StructField("s198_offline_uncorrectable", IntegerType(), True),
    StructField("s199_udma_crc_error_count", IntegerType(), True),
    StructField("s200_multi_zone_error_rate", IntegerType(), True),
    StructField("s220_disk_shift", IntegerType(), True),
    StructField("s222_loaded_hours", IntegerType(), True),
    StructField("s223_load_retry_count", IntegerType(), True),
    StructField("s226_load_in_time", IntegerType(), True),
    StructField("s240_head_flying_hours", IntegerType(), True),
    StructField("s241_total_lbas_written", IntegerType(), True),
    StructField("s242_total_lbas_read", IntegerType(), True)
])

# Read the CSV files as a data stream
streamingData = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).csv("streaming_data_dir")

# Apply a window function to the streaming data
windowedData = streamingData \
    .withWatermark("date", "31 days") \
    .groupBy(
        streamingData.vault_id,
        window(streamingData.date, "30 days", "1 day"),
        streamingData.model
    ) \
    .agg(_sum("failure").alias("total_failures"))

# Select only the date and vault_id fields
selectedData = windowedData.select("window.start", "vault_id", "total_failures")
filteredData = selectedData.filter(selectedData.total_failures > 0)

# Write the windowed data stream out to a memory sink for testing
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
