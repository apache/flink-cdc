#!/usr/bin/env bash
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

#
# This file contains tooling for compiling Flink
#

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
    exit 1  # fail
fi
CI_DIR="$HERE/../ci"
MVN_CLEAN_COMPILE_OUT="/tmp/clean_compile.out"

# source required ci scripts
source "${CI_DIR}/stage.sh"
source "${CI_DIR}/maven-utils.sh"

echo "Maven version:"
run_mvn -version

echo "=============================================================================="
echo "Compiling Flink CDC"
echo "=============================================================================="

EXIT_CODE=0

run_mvn clean package -Dmaven.javadoc.skip=true -U -DskipTests | tee $MVN_CLEAN_COMPILE_OUT

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Compiling Flink CDC failed."
    echo "=============================================================================="

    grep "0 Unknown Licenses" target/rat.txt > /dev/null

    if [ $? != 0 ]; then
        echo "License header check failure detected. Printing first 50 lines for convenience:"
        head -n 50 target/rat.txt
    fi

    exit $EXIT_CODE
fi

exit $EXIT_CODE

