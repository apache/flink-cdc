FROM --platform=linux/amd64 flink:1.18.1-java8

ARG FLINK_DIR=/opt/flink
ARG FLINK_LIB_DIR=$FLINK_DIR/lib

WORKDIR $FLINK_LIB_DIR
COPY flink-cdc-dist-dependencies/target/flink-cdc-dist-dependencies-3.2-SNAPSHOT.jar ./
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/target/flink-cdc-pipeline-connector-mysql-3.2-SNAPSHOT.jar ./
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-starrocks/target/flink-cdc-pipeline-connector-starrocks-3.2-SNAPSHOT.jar ./
# oss
RUN wget -O flink-oss-fs-hadoop-1.18.1.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-oss-fs-hadoop/1.18.1/flink-oss-fs-hadoop-1.18.1.jar"
#RUN wget -O hadoop-hdfs-client-3.3.6.jar \
#    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar"
# flink-shaded-hadoop-3-uber-3.1.3.jar
WORKDIR $FLINK_DIR

# docker build --platform linux/amd64 -f Dockerfile -t registry.cn-shenzhen.aliyuncs.com/mmg-sys/flink-cdc-pipeline-mysql-starrocks:1.18.1-java8-cdc3.2-1 .
