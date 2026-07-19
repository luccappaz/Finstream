FROM apache/flink:2.2-java21

# JARs existentes (Paimon, Kafka connector, etc.)
COPY target/flink-lib/*.jar /opt/flink/lib/

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Instala Python e PyFlink
USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages \
    apache-flink==2.2.0 \
    aiohttp \
    requests

USER flink
