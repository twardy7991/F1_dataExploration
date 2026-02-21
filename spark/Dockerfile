
## got from https://medium.com/@dkalouris/setting-up-spark-using-docker-59db2d073487
FROM python:3.11-bullseye

RUN apt-get update
RUN apt-get install -y openjdk-17-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}
# Fetch latest spark 3.5.x version and download the appropriate binary file
RUN curl -s https://dlcdn.apache.org/spark/ | grep -oP 'spark-3\.5\.[0-9]+' | sort -V | tail -1 | \
    xargs -I {} curl -O https://dlcdn.apache.org/spark/{}/{}-bin-hadoop3.tgz

 # Unpack the file and cleanup the binary file
RUN tar xvzf spark-3.5.*-bin-hadoop*.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.*-bin-hadoop*.tgz

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"
COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

RUN pip install pandas dtaidistance fastf1 pyarrow

RUN mkdir /opt/spark/events

ENTRYPOINT ["/bin/bash"]