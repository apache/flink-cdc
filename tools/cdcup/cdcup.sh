#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Do not continue after error
set -e

display_help() {
  echo "Usage: ./cdcup.sh { init | up | pipeline <yaml> | flink | mysql | stop | down | help }"
  echo
  echo "Commands:"
  echo "    * init:"
  echo "        Initialize a playground environment, and generate configuration files."
  echo
  echo "    * up:"
  echo "        Start docker containers. This may take a while before database is ready."
  echo
  echo "    * pipeline <yaml>:"
  echo "        Submit a YAML pipeline job."
  echo
  echo "    * flink:"
  echo "        Print Flink Web dashboard URL."
  echo
  echo "    * mysql:"
  echo "        Open MySQL console."
  echo
  echo "    * stop:"
  echo "        Stop all running playground containers."
  echo
  echo "    * down:"
  echo "        Stop and remove containers, networks, and volumes."
  echo
  echo "    * help:"
  echo "        Print this message."
}

if [ "$1" == 'init' ]; then
  printf "🚩 Building bootstrap docker image...\n"
  docker build -q -t cdcup/bootstrap .
  rm -rf cdc && mkdir -p cdc
  printf "🚩 Starting bootstrap wizard...\n"
  docker run -it --rm -v "$(pwd)/cdc":/cdc cdcup/bootstrap
  mv cdc/docker-compose.yaml ./docker-compose.yaml
  mv cdc/pipeline-definition.yaml ./pipeline-definition.yaml
elif [ "$1" == 'up' ]; then
  printf "🚩 Starting playground...\n"
  docker compose up -d
  docker compose exec jobmanager bash -c 'rm -rf /opt/flink-cdc'
  docker compose cp cdc jobmanager:/opt/flink-cdc
elif [ "$1" == 'pipeline' ]; then
  if [ -z "$2" ]; then
    printf "Usage: ./cdcup.sh pipeline <pipeline-definition.yaml>\n"
    exit 1
  fi
  printf "🚩 Submitting pipeline job...\n"
  docker compose cp "$2" jobmanager:/opt/flink-cdc/pipeline-definition.yaml
  startup_script="cd /opt/flink-cdc && ./bin/flink-cdc.sh ./pipeline-definition.yaml --flink-home /opt/flink"
  if test -f ./cdc/lib/hadoop-uber.jar; then
      startup_script="$startup_script --jar lib/hadoop-uber.jar"
  fi
  if test -f ./cdc/lib/mysql-connector-java.jar; then
      startup_script="$startup_script --jar lib/mysql-connector-java.jar"
  fi
  docker compose exec jobmanager bash -c "$startup_script"
elif [ "$1" == 'flink' ]; then
  port_info="$(docker compose port jobmanager 8081)"
  printf "🚩 Visit Flink Dashboard at: http://localhost:%s\n" "${port_info##*:}"
elif [ "$1" == 'mysql' ]; then
  docker compose exec -it mysql bash -c "mysql -uroot" || echo "❌ Unable to find MySQL container."
elif [ "$1" == 'stop' ]; then
  printf "🚩 Stopping playground...\n"
  docker compose stop
elif [ "$1" == 'down' ]; then
  printf "🚩 Purging playground...\n"
  docker compose down -v
else
  display_help
fi
