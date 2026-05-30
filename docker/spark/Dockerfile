FROM python:3.11-slim

ARG SPARK_VERSION=3.5.8
ARG HADOOP_PROFILE=hadoop3

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    default-jre-headless \
    default-mysql-client \
    procps \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl --fail --location --show-error \
    "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    --output /tmp/spark.tgz \
    && mkdir -p /opt \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

WORKDIR /opt/project

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash", "-lc", "tail -f /dev/null"]
