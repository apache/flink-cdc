#!/usr/bin/env bash
#
# Build full flink-cdc with Java 17 (sdkman), producing both compat-flink1 and compat-flink2 artifacts.
# Usage: ./build-with-java17.sh [skip-tests]
#   skip-tests: pass any non-empty argument to skip tests (e.g. ./build-with-java17.sh 1)
#

set -e

# Use Java 17 via sdkman (no-op if already Java 17)
if [[ -n "$SDKMAN_DIR" ]]; then
    echo "Using sdkman: switching to Java 17..."
    source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    # Prefer Temurin 17; adjust to your installed version (sdk list java)
    sdk use java 17.0.18-tem 2>/dev/null || sdk use java 17.0.11-tem 2>/dev/null || sdk use java 17.0.9-tem 2>/dev/null || true
fi

if [[ -z "$JAVA_HOME" ]] || ! java -version 2>&1 | grep -q 'version "17'; then
    echo "WARNING: Java 17 not detected. Set JAVA_HOME or run: sdk use java 17.x.x-tem"
    java -version 2>&1 || true
fi

MVN="${MVN:-$HOME/apache-maven-3.8.9/bin/mvn}"
if [[ ! -x "$MVN" ]]; then
    MVN="mvn"
fi

SKIP_TESTS=""
if [[ -n "$1" ]]; then
    SKIP_TESTS="-DskipTests -Drat.skip=true"
    echo "Build will skip tests (and RAT)."
fi

# Java 17 overrides for default build (Flink 1.20 + compat-flink1)
JAVA17="-Djava.version=17 -Dsource.java.version=17 -Dtarget.java.version=17"

echo "=========================================="
echo "Step 1: Full build (Flink 1.20, compat-flink1, Java 17)"
echo "=========================================="
"$MVN" clean install $JAVA17 $SKIP_TESTS -q

echo ""
echo "Step 2: Build Flink 2.2 compat and dist (no clean, keeps 1.20 artifacts)"
echo "=========================================="
"$MVN" install -Pflink-2.2 -pl flink-cdc-flink-compat/flink-cdc-flink-compat-flink2,flink-cdc-dist $SKIP_TESTS -q

echo ""
echo "Done. Key artifacts:"
echo "  Flink 1.20: flink-cdc-dist/target/flink-cdc-dist-*-1.20.jar"
echo "  Flink 2.2:  flink-cdc-dist/target/flink-cdc-dist-*-2.2.jar"
ls -la flink-cdc-dist/target/flink-cdc-dist-*.jar 2>/dev/null || true
