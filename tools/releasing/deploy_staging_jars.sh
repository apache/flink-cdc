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
# Deployment strategy (two invocations, one per Flink line):
#   - Flink 1.20 line: full reactor deploy. This deploys the intermediate modules
#     (common/runtime/composer/cli/python, the thin pipeline connector code modules
#     and the non-forking pipeline connector fat jars) exactly once; their content
#     is generation-equivalent (verified by jar-level comparison), so the single
#     copy serves both Flink lines.
#   - Flink 2.2 line: full reactor build (install), then a deploy restricted to
#     the per-version modules whose artifactIds carry the Flink line (dist-flink-2.2,
#     the sql-connector-*-flink-2.2 fat jars and the forking pipeline connector
#     fat jars). The restricted deploy resolves everything else from the local
#     repository; -am must NOT be used here or the shared-GAV intermediate modules
#     would be deployed a second time and rejected by the staging repository.
#     The restriction also keeps the flink2-only thin starrocks/hudi jars
#     (connector.generation.skip) out of the staging repository.
# The restriction is applied via a default -pl list; override with CUSTOM_OPTIONS
# if a different module set is needed.
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
        if [ -z "${CUSTOM_OPTIONS:-}" ]; then
            per_version_modules="flink-cdc-dist"
            for connector in db2 mongodb mysql oceanbase oracle postgres sqlserver tidb vitess; do
                per_version_modules="${per_version_modules},flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-${connector}-cdc"
            done
            for connector in elasticsearch iceberg kafka paimon; do
                per_version_modules="${per_version_modules},flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-${connector}"
            done
            CUSTOM_OPTIONS="-pl '${per_version_modules}'"
        fi
        # Full flink2 build first: populates the local repository with the
        # flink2-form artifacts the restricted deploy resolves from.
        echo "Building the full flink2 reactor (install, no deploy)"
        ${MVN} clean install ${MVN_OPTIONS} -DskipTests
    fi
fi

echo "Deploying to repository.apache.org"
${MVN} clean deploy ${MVN_OPTIONS} ${CUSTOM_OPTIONS}

# Transition-period convenience: also emit legacy-named jars
# (flink-cdc-dist-<rev>-<flinkMajor>.jar, flink-sql-connector-<X>-cdc-<rev>-<flinkMajor>.jar)
# next to the per-version artifacts, keeping download links from the version-suffix era working.
if [ -n "${FLINK_VERSION:-}" ]; then
    legacy_dir="${PROJECT_ROOT}/release-artifacts"
    mkdir -p "${legacy_dir}"

    dist_jar="flink-cdc-dist/target/flink-cdc-dist-flink-${flink_major_version}-${RELEASE_VERSION}.jar"
    if [ -f "${dist_jar}" ]; then
        cp "${dist_jar}" "${legacy_dir}/flink-cdc-dist-${RELEASE_VERSION}-${flink_major_version}.jar"
        echo "Legacy-named dist jar written to ${legacy_dir}"
    fi

    # Per-version source connector fat jars: the active Flink line's
    # shaded jar is also copied under the legacy version-suffix name.
    for connector_dir in flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-*-cdc; do
        connector=$(basename "${connector_dir}")
        fat_jar="${connector_dir}/target/${connector}-flink-${flink_major_version}-${RELEASE_VERSION}.jar"
        if [ -f "${fat_jar}" ]; then
            cp "${fat_jar}" "${legacy_dir}/${connector}-${RELEASE_VERSION}-${flink_major_version}.jar"
        fi
    done
    echo "Legacy-named source connector jars written to ${legacy_dir}"

    # Per-version pipeline connector fat jars (kafka/paimon/elasticsearch/iceberg):
    # the active Flink line's shaded jar is also copied under the legacy
    # version-suffix name, replacing the single-GAV fat jar of the 3.7 era.
    for connector in elasticsearch iceberg kafka paimon; do
        connector_module="flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-${connector}"
        fat_jar="${connector_module}/target/flink-cdc-pipeline-connector-${connector}-flink-${flink_major_version}-${RELEASE_VERSION}.jar"
        if [ -f "${fat_jar}" ]; then
            cp "${fat_jar}" "${legacy_dir}/flink-cdc-pipeline-connector-${connector}-${RELEASE_VERSION}-${flink_major_version}.jar"
        fi
    done
    echo "Legacy-named pipeline connector jars written to ${legacy_dir}"
fi

cd ${CURR_DIR}