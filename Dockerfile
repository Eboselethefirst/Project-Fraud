# Start with a clean Python 3.11 environment (Debian-based)
FROM python:3.11-slim-bookworm

# 1. Install Java (Spark needs this to breathe)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean

# 2. Set Environment Variables
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 3. Download and Install Spark directly
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm /tmp/spark.tgz

# 4. Install your ML libraries (No permission errors here!)
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    pandas \
    pyarrow \
    pyspark==${SPARK_VERSION} \
    kafka-python==2.0.2 \
    python-dotenv

WORKDIR /opt/spark/work-dir