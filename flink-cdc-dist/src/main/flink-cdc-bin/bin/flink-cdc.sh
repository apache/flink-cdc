#!/usr/bin/env bash
################################################################################
#  Copyright 2023 Ververica Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Setup FLINK_HOME
args=("$@")
# Loop through command-line arguments
for ((i=0; i < ${#args[@]}; i++)); do
    case "${args[i]}" in
        --flink-home)
            if [[ -n "${args[i+1]}" ]]; then
                FLINK_HOME="${args[i+1]}"
                break
            fi
            ;;
    esac
done

# Define directories
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FLINK_CDC_HOME="$SCRIPT_DIR"/..
FLINK_CDC_CONF="$FLINK_CDC_HOME"/conf
FLINK_CDC_LIB="$FLINK_CDC_HOME"/lib
FLINK_CDC_LOG="$FLINK_CDC_HOME"/log

# Build Java classpath
CLASSPATH=""
# Add Flink libraries to the classpath
for jar in "$FLINK_HOME"/lib/*.jar; do
  CLASSPATH=$CLASSPATH:$jar
done
# Add Flink CDC libraries to classpath
for jar in "$FLINK_CDC_LIB"/*.jar; do
  CLASSPATH=$CLASSPATH:$jar
done
# Trim classpath
CLASSPATH=${CLASSPATH#:}

# Setup Java by operating system
UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d "$JAVA_HOME" ]]; then
        JAVA_RUN="$JAVA_HOME"/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Setup logging
LOG=$FLINK_CDC_LOG/flink-cdc-cli-$HOSTNAME.log
LOG_SETTINGS=(-Dlog.file="$LOG" -Dlog4j.configuration=file:"$FLINK_CDC_CONF"/log4j-cli.properties -Dlog4j.configurationFile=file:"$FLINK_CDC_CONF"/log4j-cli.properties)

exec "$JAVA_RUN" -classpath "$CLASSPATH" "${LOG_SETTINGS[@]}" com.ververica.cdc.cli.CliFrontend "$@"
