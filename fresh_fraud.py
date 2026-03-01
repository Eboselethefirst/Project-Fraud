from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when,ceil
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import LongType, StringType, StructType, StructField, DoubleType, IntegerType
from pyspark.ml.pipeline import PipelineModel
import numpy as np


spark = SparkSession.builder.appName("Nigerian Real Time Fraud System").getOrCreate()

model = PipelineModel.load("/opt/spark/work-dir/fraud_ml_model")

schema = StructType([
    StructField("step", IntegerType()), StructField("type", StringType()),
    StructField("amount", DoubleType()), StructField("sender_acc_no", StringType()),
    StructField("bank_code", StringType()), StructField("home_state", StringType()),
    StructField("origin_state", StringType()), StructField("is_night_tx", IntegerType()),
    StructField("velocity_5m", IntegerType()), StructField("loc_change_speed", IntegerType()),
])


raw_stream= spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-kraft:9092").option("subscribe", "raw_transactions").load()

df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

df1= df.withColumn("amount", ceil(col("amount")))

scored_df = model.transform(df1)

final_df = scored_df.withColumn("fraud_prob", vector_to_array(col("probability")).getItem(1)) \
    .withColumn("action",
        when(col("fraud_prob") >= 0.8, "BLOCK AND ALARM")
        .when(col("fraud_prob") >=0.3, "FLAG TO COMPLIANCE")
        .otherwise("ALLOW")    )

query = final_df.select("sender_acc_no", "amount", "type","bank_code", "fraud_prob","action") \
    .writeStream.outputMode("append").format("console").start()

query.awaitTermination()
