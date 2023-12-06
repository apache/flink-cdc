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
# Check if FLINK_HOME is set in command-line arguments by "--flink-home"
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
if [[ -z $FLINK_HOME ]]; then
  echo "[ERROR] Unable to find FLINK_HOME either in command-line argument \"--flink-home\" or environment variable \"FLINK_HOME\"."
  exit 1
fi

# Setup Flink related configurations
# Setting _FLINK_HOME_DETERMINED in order to avoid config.sh to overwrite it
_FLINK_HOME_DETERMINED=1
# FLINK_CONF_DIR is required by config.sh
FLINK_CONF_DIR=$FLINK_HOME/conf
# Use config.sh to setup Flink related configurations
. $FLINK_HOME/bin/config.sh

# Define Flink CDC directories
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FLINK_CDC_HOME="$SCRIPT_DIR"/..
export FLINK_CDC_HOME=$FLINK_CDC_HOME
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
# Add Hadoop classpath, which is defined in config.sh
CLASSPATH=$CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS
# Trim classpath
CLASSPATH=${CLASSPATH#:}

# Setup logging
LOG=$FLINK_CDC_LOG/flink-cdc-cli-$HOSTNAME.log
LOG_SETTINGS=(-Dlog.file="$LOG" -Dlog4j.configuration=file:"$FLINK_CDC_CONF"/log4j-cli.properties -Dlog4j.configurationFile=file:"$FLINK_CDC_CONF"/log4j-cli.properties)

# JAVA_RUN should have been setup in config.sh
exec "$JAVA_RUN" -classpath "$CLASSPATH" "${LOG_SETTINGS[@]}" com.ververica.cdc.cli.CliFrontend "$@"
