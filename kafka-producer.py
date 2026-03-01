import pandas as pd
import json
import time 
from kafka import KafkaProducer 


producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')

)

TOPIC = "fraud_transactions"
CSV_PATH = "./data/unprocessed/nigerian_fraud_data_100k.csv"
TARGET_TPS = 1000

df = pd.read_csv(CSV_PATH)
total_rows = len(df)
print(f"Loaded {total_rows}, Starting Ingestion at {TARGET_TPS} TPS...")

start_time = time.time()
for i, row in df.iterrows():
    message = row.to_dict()
    producer.send(TOPIC, value=message)

    if (i + 1) % TARGET_TPS == 0:
        elapsed= time.time() - start_time
        #if we sent 1000 rows faster than 1 second sleep the difference 
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        
        #Reset timer for next batch 
        print(f"Sent {i+1}/{total_rows} transactions....")
        start_time = time.time()

producer.flush()

print("Ingestion_Complete!!")
