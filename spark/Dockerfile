FROM apache/spark:3.5.1

USER root

# Create Spark conf directory
RUN mkdir -p /opt/spark/conf

# Install wget
RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

# Download Delta Lake jars
RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Copy Hive configuration
COPY spark-conf/hive-site.xml /opt/spark/conf/hive-site.xml

USER spark