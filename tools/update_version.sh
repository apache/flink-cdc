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

###########################

cd ${PROJECT_ROOT}

# Check if NEW_VERSION is set
if [ -z "${NEW_VERSION:-}" ]; then
    echo "Error: NEW_VERSION environment variable is not set."
    echo "Usage: NEW_VERSION=3.8.0 $0"
    exit 1
fi

# Update the project revision version. Per-version dist artifacts are
# distinguished by artifactId (flink-cdc-dist-flink-<ver>), so the project
# version no longer carries a Flink suffix.
current_version=$(get_pom_version)
echo "Current version: ${current_version}"
echo "New version: ${NEW_VERSION}"

${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set-property -Dproperty=revision -DnewVersion=${NEW_VERSION} -DgenerateBackupPoms=false --quiet

echo "Version updated successfully: ${current_version} -> ${NEW_VERSION}"

cd ${CURR_DIR}
