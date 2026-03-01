from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when,ceil
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import LongType, StringType, StructType, StructField, DoubleType, IntegerType, BooleanType
from pyspark.ml.pipeline import PipelineModel
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_url = os.getenv("POSTGRES_URL")

spark = SparkSession.builder.appName("Nigerian Real Time Fraud Detector").getOrCreate()

model = PipelineModel.load("/opt/spark/work-dir/fraud_ml_model")

schema = StructType([
    StructField("step", IntegerType()), StructField("type", StringType()),
    StructField("amount", DoubleType()), StructField("sender_acc_no", StringType()),
    StructField("bank_code", StringType()), StructField("home_state", StringType()),
    StructField("origin_state", StringType()), StructField("is_night_tx", BooleanType()),
    StructField("velocity_5m", IntegerType()), StructField("loc_change_speed", IntegerType()),
    StructField("isFraud", IntegerType())
])


raw_stream= spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-kraft:29092").option("subscribe", "fraud_transactions").load()

df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

#df1= df.withColumn("amount", ceil(col("amount")))

df_clean = df.na.drop(subset=["amount", "type","is_night_tx","velocity_5m", "loc_change_speed"])
df_edit = df_clean.withColumn("is_night_tx", col("is_night_tx").cast("int"))

scored_df = model.transform(df_edit)
print("Columns for the trained dataset:", scored_df.columns)

final_df = scored_df.withColumn("fraud_prob", vector_to_array(col("probability")).getItem(1)) \
    .withColumn("action",
        when(col("fraud_prob") >= 0.8, "BLOCK AND ALARM")
        .when(col("fraud_prob") >=0.3, "FLAG TO COMPLIANCE")
        .otherwise("ALLOW")    )


def write_to_sinks(batch_df, batch_id):

    print(f"DEBUG: Processing Batch ID: {batch_id}, Row Count: {batch_df.count()}")
    
    if batch_df.count() == 0:
        print("WARNING: Received an empty batch. Check your Kafka Producer/Schema!")
        return
    batch_df.persist()
    

    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "transaction_data") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append").save()
    
    compliance_team = batch_df.filter(col("action") != "ALLOW")
    if compliance_team.count() > 0:

        compliance_team.write.mode("append").option("header","true").csv("/opt/spark/work-dir/compliance_alerts/")
    batch_df.unpersist()
    
query = final_df.select("step","type","amount","sender_acc_no","bank_code","home_state","origin_state","is_night_tx","velocity_5m","loc_change_speed","isFraud","fraud_prob","action").writeStream.foreachBatch(write_to_sinks).start()

query.awaitTermination()

