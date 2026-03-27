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
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

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

function set_revision_property {
  new_revision=$1

  ${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set-property -Dproperty=revision -DnewVersion=${new_revision} -DgenerateBackupPoms=false --quiet
}

###########################

cd ${PROJECT_ROOT}

# Build Maven options
MVN_OPTIONS="-Papache-release -DskipTests -DretryFailedDeploymentCount=10"

# If FLINK_VERSION is set, pass it to Maven
if [ -n "${FLINK_VERSION:-}" ]; then
    echo "Using Flink version: ${FLINK_VERSION}"

    # Extract and set flink.major.version (e.g., "1.20" from "1.20.3")
    flink_major_version=$(echo "${FLINK_VERSION}" | cut -d. -f1,2)
    MVN_OPTIONS="${MVN_OPTIONS} -Dflink.major.version=${flink_major_version}"

    # Update project version: RELEASE_VERSION-FLINK_VERSION
    current_version=$(get_pom_version)
    echo "Current version: ${current_version}"

    if [ -z "${RELEASE_VERSION:-}" ]; then
        echo "ERROR: RELEASE_VERSION environment variable is required"
        exit 1
    fi
    new_version="${RELEASE_VERSION}-${FLINK_VERSION}"

    # Skip if version already matches RELEASE_VERSION-FLINK_VERSION
    if [[ "${current_version}" == "${new_version}" ]]; then
        echo "Version already is ${new_version}, skipping version update"
    else
        echo "Updating version to: ${new_version}"
        set_pom_version "${new_version}"
        set_revision_property "${new_version}"
    fi

    # Add -Pflink2 profile if FLINK_VERSION starts with "2"
    if [[ "${FLINK_VERSION}" == 2* ]]; then
        echo "Enabling flink2 profile"
        MVN_OPTIONS="${MVN_OPTIONS} -Pflink2"
    fi
fi

echo "Deploying to repository.apache.org"
${MVN} clean deploy ${MVN_OPTIONS} ${CUSTOM_OPTIONS}

cd ${CURR_DIR}