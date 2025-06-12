#/*
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *      http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

FROM flink

ARG FLINK_CDC_VERSION=3.5-SNAPSHOT
ARG PIPELINE_DEFINITION_FILE

RUN mkdir -p /opt/flink-cdc
RUN mkdir -p /opt/flink/usrlib
ENV FLINK_CDC_HOME /opt/flink-cdc
COPY flink-cdc-dist/target/flink-cdc-${FLINK_CDC_VERSION}-bin.tar.gz /tmp/
RUN tar -xzvf /tmp/flink-cdc-${FLINK_CDC_VERSION}-bin.tar.gz -C /tmp/ && \
    mv /tmp/flink-cdc-${FLINK_CDC_VERSION}/* /opt/flink-cdc/ && \
    mv /opt/flink-cdc/lib/flink-cdc-dist-${FLINK_CDC_VERSION}.jar /opt/flink-cdc/lib/flink-cdc-dist.jar && \
    rm -rf /tmp/flink-cdc-${FLINK_CDC_VERSION} /tmp/flink-cdc-${FLINK_CDC_VERSION}-bin.tar.gz
# copy jars to cdc libs
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-values/target/flink-cdc-pipeline-connector-values-${FLINK_CDC_VERSION}.jar /opt/flink/usrlib/flink-cdc-pipeline-connector-values-${FLINK_CDC_VERSION}.jar
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/target/flink-cdc-pipeline-connector-mysql-${FLINK_CDC_VERSION}.jar /opt/flink/usrlib/flink-cdc-pipeline-connector-mysql-${FLINK_CDC_VERSION}.jar
# copy flink cdc pipeline conf file, Here is an example. Users can replace it according to their needs.
COPY $PIPELINE_DEFINITION_FILE $FLINK_CDC_HOME/conf
