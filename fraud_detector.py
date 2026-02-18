from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark= SparkSession.builder \
    .appName("Fraud_velocity_check").config("spark.hadoop.fs.local.block.size", "33554432") \
    .config("spark.hadoop.fs.permissions.umask-mode", "000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Defining The Schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType()),
    StructField("location", StringType())
])

raw_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-kraft:29092").option("subscribe", "raw_transactions").option("startingOffsets", "earliest").load()
    

transactions = raw_stream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schema).alias("data")) \
    .select("data.*")

fraudulent_activity = transactions \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"), # Slide every 30s
        col("card_id")
    ) \
    .agg(count("transaction_id").alias("tx_count")) \
    .filter(col("tx_count") > 3)

query = fraudulent_activity.writeStream.outputMode("complete").format("console") \
    .option("checkpointLocation", "/tmp/checkpoints").start()

query.awaitTermination()
