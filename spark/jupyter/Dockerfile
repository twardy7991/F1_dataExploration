
FROM apache/spark-py:latest

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install jupyter notebook numpy pandas pyarrow