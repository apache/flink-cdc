#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
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

# override env to use Java 11 to for build instead default Java 8
# path to JDK is taken from https://github.com/apache/flink-connector-shared-utils/blob/ci_utils/docker/base/Dockerfile#L37-L40
if [ -n "${JAVA_HOME_11_X64:-}" ]; then
    export JAVA_HOME=$JAVA_HOME_11_X64
    export PATH=$JAVA_HOME_11_X64/bin:$PATH
fi

mvn --version
java -version
javadoc -J-version

# workaround for a git security patch
git config --global --add safe.directory /root/flink-cdc

# CI builds the site on the host before generating Javadoc in the Java container.
if [ "${DOCS_SKIP_SITE_BUILD:-false}" != "true" ]; then
    npm --prefix docs ci
    npm --prefix docs run build
fi

# build Flink; required for Javadoc step
mvn clean install -B -DskipTests -Dfast

# build java/scala docs
mkdir -p docs/target/api
# Compatibility modules target different Flink versions and cannot share a Javadoc classpath.
mvn javadoc:aggregate -B \
    -pl '!flink-cdc-flink1-compat,!flink-cdc-flink2-compat' \
    -DadditionalJOption="-Xdoclint:none --allow-script-in-comments" \
    -Dmaven.javadoc.failOnError=true \
    -Dcheckstyle.skip=true \
    -Dspotless.check.skip=true \
    -Denforcer.skip=true \
    -Dheader="<a href=\"http://flink.apache.org/\" target=\"_top\"><h1>Back to Flink Website</h1></a> <script>var _paq=window._paq=window._paq||[];_paq.push([\"disableCookies\"]),_paq.push([\"setDomains\",[\"*.flink.apache.org\",\"*.nightlies.apache.org/flink\"]]),_paq.push([\"trackPageView\"]),_paq.push([\"enableLinkTracking\"]),function(){var u=\"//matomo.privacy.apache.org/\";_paq.push([\"setTrackerUrl\",u+\"matomo.php\"]),_paq.push([\"setSiteId\",\"1\"]);var d=document, g=d.createElement('script'), s=d.getElementsByTagName('script')[0];g.async=true; g.src=u+'matomo.js'; s.parentNode.insertBefore(g,s)}();</script>"
mv target/site/apidocs docs/target/api/java
