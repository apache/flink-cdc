#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

function get_pom_version {
  set +u
  echo $(${MVN} help:evaluate -Dexpression="project.version" -q -DforceStdout)
  set -u
}

function set_pom_version {
  new_version=$1

  ${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set -DnewVersion=${new_version} -DgenerateBackupPoms=false --quiet
}

###########################

cd ${PROJECT_ROOT}

# Check if FLINK_VERSION is set
if [ -z "${FLINK_VERSION:-}" ]; then
    echo "Error: FLINK_VERSION environment variable is not set."
    echo "Usage: FLINK_VERSION=1.20 $0"
    exit 1
fi

echo "FLINK_VERSION: ${FLINK_VERSION}"

# Get current version
current_version=$(get_pom_version)
echo "Current version: ${current_version}"

# Extract base version (remove -SNAPSHOT suffix if present)
base_version=${current_version%-SNAPSHOT}
echo "Base version: ${base_version}"

# Build new version: base_version-FLINK_VERSION
new_version="${base_version}-${FLINK_VERSION}"
echo "New version: ${new_version}"

# Set new version
set_pom_version "${new_version}"

echo "Version updated successfully: ${current_version} -> ${new_version}"

cd ${CURR_DIR}
