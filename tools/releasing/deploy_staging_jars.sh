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

function set_revision_property {
  new_revision=$1

  ${MVN} org.codehaus.mojo:versions-maven-plugin:2.8.1:set-property -Dproperty=revision -DnewVersion=${new_revision} -DgenerateBackupPoms=false --quiet
}

###########################

cd ${PROJECT_ROOT}

# Build Maven options
MVN_OPTIONS="-Papache-release -DskipTests -DretryFailedDeploymentCount=10"

# Per-version dist deployment: the Flink line is carried
# by dist artifactIds (flink-cdc-dist-flink-<ver>), not by the project version, so the
# project version is no longer rewritten with a Flink suffix. Generation gating in the
# root pom makes sure only the selected line's dist module installs/deploys; invoke this
# script once per Flink line (FLINK_VERSION=1.20.3, then FLINK_VERSION=2.2.0).
#
# TODO(FLINK-XXXX) (open for the community proposal): a full reactor deploy runs once
# per Flink line, but the intermediate modules (flink-cdc-common/runtime/composer/cli/
# python and the pipeline connectors) share one GAV across generations with different
# per-generation content. Deploying both lines' full reactor into one staging
# repository will be rejected on the second run (same GAV already deployed). Until the
# community settles the deployment strategy, the second line's invocation should
# restrict the deploy to the per-version dist module via CUSTOM_OPTIONS, e.g.:
#   CUSTOM_OPTIONS="-pl 'flink-cdc-dist/flink-cdc-dist-flink-2.2 -am'"
if [ -n "${FLINK_VERSION:-}" ]; then
    echo "Using Flink version: ${FLINK_VERSION}"

    # Extract the Flink major version (e.g., "1.20" from "1.20.3") to locate the
    # per-version dist module below
    flink_major_version=$(echo "${FLINK_VERSION}" | cut -d. -f1,2)

    # Plain release version bump (no Flink suffix)
    if [ -z "${RELEASE_VERSION:-}" ]; then
        echo "ERROR: RELEASE_VERSION environment variable is required"
        exit 1
    fi
    set_revision_property "${RELEASE_VERSION}"

    # Add -Pflink2 profile if FLINK_VERSION starts with "2"
    if [[ "${FLINK_VERSION}" == 2* ]]; then
        echo "Enabling flink2 profile"
        MVN_OPTIONS="${MVN_OPTIONS} -Pflink2"
        # TODO(FLINK-XXXX): under -Pflink2 the starrocks and hudi connector
        # modules skip their shade step (connector.generation.skip=true in the
        # root pom) and would deploy thin, unusable jars into the 2.x line. The
        # dist side is covered by generation gating; until the connector
        # per-version restructuring lands, exclude them from a flink2
        # deployment via CUSTOM_OPTIONS, e.g.:
        #   CUSTOM_OPTIONS="-pl '!flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-starrocks,!flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-hudi'"
    fi
fi

echo "Deploying to repository.apache.org"
${MVN} clean deploy ${MVN_OPTIONS} ${CUSTOM_OPTIONS}

# Transition-period convenience: also emit legacy-named dist jars
# (flink-cdc-dist-<rev>-<flinkMajor>.jar) next to the per-version artifacts,
# keeping download links from the version-suffix era working.
if [ -n "${FLINK_VERSION:-}" ]; then
    dist_jar="flink-cdc-dist/target/flink-cdc-dist-flink-${flink_major_version}-${RELEASE_VERSION}.jar"
    if [ -f "${dist_jar}" ]; then
        legacy_dir="${PROJECT_ROOT}/release-artifacts"
        mkdir -p "${legacy_dir}"
        cp "${dist_jar}" "${legacy_dir}/flink-cdc-dist-${RELEASE_VERSION}-${flink_major_version}.jar"
        echo "Legacy-named dist jar written to ${legacy_dir}"
    fi
fi

cd ${CURR_DIR}