📄 GitHub Project Overview: Nigerian Real-Time Fraud Detector
🏗️ Architecture
This system simulates a high-frequency banking environment (1,000 TPS):

Ingestion: A Python producer streams Nigerian transaction data from a CSV to Kafka.

Processing: Spark Structured Streaming consumes the data, cleans it (handling missing values and casting booleans), and applies a pre-trained Random Forest ML Model.

Action Engine: High-probability fraud is flagged in real-time.

Storage (Dual-Sinks): * PostgreSQL: Stores every single processed transaction for long-term auditing.

CSV Alerts: Filters for "Action != ALLOW" and writes alerts to a specific directory for the Compliance Team to review in Excel.
