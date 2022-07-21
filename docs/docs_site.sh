#!/bin/bash
################################################################################
#  Copyright 2022 Ververica Inc.
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

set -e

docs_container_name="flink-cdc-docs_container"
docs_image_name="flink-cdc-docs"
port=8001
host=localhost

function start_docs_server() {
  project_dir="$(dirname "$(pwd)")"
  echo "starting docs server....."
  docker build -t ${docs_image_name} -f ${project_dir}/docs/Dockerfile .
  docker run -d -it -p ${port}:${port} --rm -v "${project_dir}":/home/flink-cdc --name ${docs_container_name} ${docs_image_name}
  echo "docs server is running on  http://${host}:${port}"
}

function stop_docs_server() {
  project_dir="$(dirname "$(pwd)")"
  echo "stopping docs server....."
  docker stop ${docs_container_name}
  rm -rf ${project_dir}/docs/_build
  echo "stop docs server successfully."
}

if ! command -v docker &> /dev/null
then
	echo "Docker must be installed to run the docs locally"
	echo "Please see docs/README.md for more details"
	exit 1
fi

if  [[ $1 = "start" ]]; then
  start_docs_server
elif [[ $1 = "stop" ]]; then
  stop_docs_server
else
  echo "Usage:"
  echo "$0 start"
  echo "$0 stop"
fi