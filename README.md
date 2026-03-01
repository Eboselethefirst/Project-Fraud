# Real-Time Fraud Detection Pipeline (Nigerian Fintech Scenario)

# Overview

This project implements a high-throughput, real-time fraud detection engine designed for the Nigerian banking landscape. It simulates a stream of financial transactions (including regional bank codes like OPay, Moniepoint, and Access Bank) and uses a Machine Learning model to intercept fraudulent activity before it hits the ledger.
The "So What?": In a market with high transaction volumes and evolving social engineering tactics, this pipeline provides the sub-second latency required to Block and Alarm suspicious transfers while maintaining an audit trail for compliance.

# System Architecture
The pipeline is built on a modern "Lambda-lite" architecture using a distributed streaming stack:
Ingestion (Kafka): A Python-based producer streams 1,000+ Transactions Per Second (TPS) from an 800k+ row dataset.
Processing (Spark Structured Streaming): * Data Cleaning: Real-time schema validation and type-casting (Boolean to Integer).
Inference: A pre-trained Random Forest Pipeline Model scores each transaction.
Action Logic: Transactions are categorized as ALLOW, FLAG TO COMPLIANCE, or BLOCK AND ALARM based on probability thresholds.
Storage (Dual-Sinks):
PostgreSQL: Comprehensive transaction logs for long-term auditing and model retraining.
CSV Alerts: High-risk transactions are offloaded to a dedicated directory for immediate manual review.

# Professional Features
Zero-Trust Security: Credential management is handled via python-dotenv. No sensitive database passwords (puffpuff) are hardcoded or committed to version control.
Memory Efficiency: Implements .persist() and .unpersist() patterns within micro-batches to prevent memory leaks and redundant computation when writing to multiple sinks.
Financial Precision: Strictly maintains DoubleType precision for transaction amounts to ensure audit-grade accuracy (no rounding errors).
Containerized Orchestration: The entire environment (Kafka, Zookeeper, Spark, Postgres) is managed via docker-compose for 1-click reproducibility.


# Setup & Installation

1. Clone and Configure

Bash

git clone https://github.com/Eboselethefirst/Project-Fraud.git

cd Project-Fraud

2. Set Environment Variables
Create a .env file in the root:

Plaintext

POSTGRES_USER=admin

POSTGRES_PASSWORD=your_password

POSTGRES_DB=fraud_db

POSTGRES_URL=jdbc:postgresql://postgres_fraud:5432/fraud_db

3. Launch Infrastructure

Bash

docker-compose up -d

4. Start the Engine

Bash

In one terminal, start the producer

python kafka-producer.py

In another terminal, submit the Spark job

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 fresh_fraud.py

# Data Schema
 Column	Type	Description
step	Int	Unit of time (1 step = 1 hour)
bank_code	String	Nigerian Bank (e.g., OPay, Moniepoint)
is_night_tx	Boolean	Binary flag for 12am-5am transactions
fraud_prob	Double	ML Model confidence score (0.0 - 1.0)
action	String	Business logic result (BLOCK/FLAG/ALLOW)


# Lessons Learned & Engineering Challenges
Schema Evolution: Resolved a NOT NULL constraint violation in Postgres by aligning the Spark .select() statement with the database DDL.
Type Mismatch: Handled Boolean/Integer conversion issues between Kafka JSON serialization and Spark ML expectations.
Checkpointing: Mastered the use of Spark Checkpoint metadata to ensure fault-tolerant processing across job restarts.


# Fraud Impact Report (SQL Insights)
To demonstrate the value of this pipeline, run the following query in pgAdmin/Postgres to see the total potential loss prevented:

SQL

-- Calculate Total Naira Saved from Blocked Transactions


SELECT 
    action, 
    COUNT(*) as total_count, 
    SUM(amount) as total_naira_value
FROM transactions_log
GROUP BY action;

