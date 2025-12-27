#!/bin/bash

# ==============================================================================
# GaussDB to GaussDB CDC Connector Deployment Script
# 专门用于 GaussDB -> GaussDB 同步场景的部署
# 使用 gaussdbjdbc.jar 驱动（同时作为 Source 和 Sink 驱动）
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project paths
PROJECT_ROOT=$(pwd)
CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc"
SQL_CONNECTOR_MODULE="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc"
JDBC_DRIVER="$PROJECT_ROOT/$CONNECTOR_MODULE/lib/gaussdbjdbc.jar"
FLINK_JDBC_CONNECTOR="$PROJECT_ROOT/$CONNECTOR_MODULE/lib/flink-connector-jdbc.jar"
SQL_FILE="${SQL_FILE:-$PROJECT_ROOT/$CONNECTOR_MODULE/docker/sql/gaussdb_distributed_to_gaussdb.sql}"

echo "🚀 Starting GaussDB -> GaussDB CDC deployment process..."

# 0. 强制清理 Maven 缓存以确保使用最新代码
echo "🧹 Cleaning Maven cache to ensure fresh build..."
mvn clean -pl $CONNECTOR_MODULE,$SQL_CONNECTOR_MODULE

# 1. Build project with forced recompilation
echo "📦 Building GaussDB CDC connector (forced fresh build)..."
echo "   Building base connector..."
mvn clean install -DskipTests \
    -Drat.skip \
    -Dspotless.skip=true \
    -Dspotless.check.skip=true \
    -Dspotless.apply.skip=true \
    -Dcheckstyle.skip=true \
    -pl $CONNECTOR_MODULE \
    -am

echo "   Building SQL connector..."
mvn clean install -DskipTests \
    -Drat.skip \
    -Dspotless.skip=true \
    -Dspotless.check.skip=true \
    -Dspotless.apply.skip=true \
    -Dcheckstyle.skip=true \
    -pl $SQL_CONNECTOR_MODULE \
    -am

CONNECTOR_JAR="$PROJECT_ROOT/$SQL_CONNECTOR_MODULE/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar"

if [ ! -f "$CONNECTOR_JAR" ]; then
    echo -e "${RED}❌ Error: Connector JAR not found at $CONNECTOR_JAR${NC}"
    exit 1
fi

# 验证 JAR 包是最新构建的（5分钟内）
JAR_AGE=$(($(date +%s) - $(stat -f %m "$CONNECTOR_JAR" 2>/dev/null || stat -c %Y "$CONNECTOR_JAR")))
if [ $JAR_AGE -gt 300 ]; then
    echo -e "${YELLOW}⚠️  Warning: JAR file is older than 5 minutes (${JAR_AGE}s old)${NC}"
    echo -e "${YELLOW}   This might indicate the build used cached artifacts${NC}"
fi

echo -e "${GREEN}✅ JAR built successfully: $(ls -lh $CONNECTOR_JAR | awk '{print $5}')${NC}"

# 验证 JDBC 驱动存在
if [ ! -f "$JDBC_DRIVER" ]; then
    echo -e "${RED}❌ Error: GaussDB JDBC driver not found at $JDBC_DRIVER${NC}"
    exit 1
fi
echo -e "${GREEN}✅ GaussDB JDBC driver found: gaussdbjdbc.jar${NC}"

# 2. Create modified flink-connector-jdbc.jar with GaussDB dialect
echo "🔧 Creating modified JDBC connector with GaussDB dialect..."
JDBC_MOD_DIR="/tmp/jdbc_mod_$$"
rm -rf "$JDBC_MOD_DIR" && mkdir -p "$JDBC_MOD_DIR" && cd "$JDBC_MOD_DIR"

# Extract existing flink-connector-jdbc.jar
docker cp flink-jobmanager:/opt/flink/lib/flink-connector-jdbc.jar . 2>/dev/null || \
docker cp flink-taskmanager:/opt/flink/lib/flink-connector-jdbc.jar . || \
cp "$FLINK_JDBC_CONNECTOR" flink-connector-jdbc.jar

unzip -q -o flink-connector-jdbc.jar -d extracted

# Add GaussDB dialect factory to SPI file
echo "org.apache.flink.cdc.connectors.gaussdb.jdbc.GaussDBJdbcDialectFactory" >> extracted/META-INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory

# Copy GaussDB dialect classes
mkdir -p extracted/org/apache/flink/cdc/connectors/gaussdb/jdbc
cp "$PROJECT_ROOT/$CONNECTOR_MODULE/target/classes/org/apache/flink/cdc/connectors/gaussdb/jdbc/"*.class extracted/org/apache/flink/cdc/connectors/gaussdb/jdbc/

# Repackage
cd extracted && jar -cf ../flink-connector-jdbc-gaussdb.jar . && cd ..

echo -e "${GREEN}✅ Modified JDBC connector created with GaussDB dialect${NC}"
cd "$PROJECT_ROOT"

# 3. Deploy to Flink Cluster
echo "🚚 Distributing JARs to Flink cluster containers..."
for container in flink-jobmanager flink-taskmanager; do
    echo "  -> Deploying to $container..."

    # 先彻底删除所有可能冲突的 JAR 包
    docker exec $container bash -c "rm -f /opt/flink/lib/flink-connector-gaussdb-cdc-*.jar \
                                       /opt/flink/lib/flink-sql-connector-gaussdb-cdc-*.jar \
                                       /opt/flink/lib/gaussdbjdbc.jar \
                                       /opt/flink/lib/gsjdbc4.jar \
                                       /opt/flink/lib/mysql-connector-*.jar \
                                       /opt/flink/lib/flink-connector-jdbc*.jar \
                                       /opt/flink/lib/gaussdb-jdbc-dialect.jar \
                                       /opt/flink/usrlib/*.jar" || true

    # Copy Connector, JDBC Driver, and modified JDBC Connector
    docker cp "$CONNECTOR_JAR" $container:/opt/flink/lib/
    docker cp "$JDBC_DRIVER" $container:/opt/flink/lib/
    docker cp "$JDBC_MOD_DIR/flink-connector-jdbc-gaussdb.jar" $container:/opt/flink/lib/flink-connector-jdbc.jar
done

# Cleanup temp directory
# rm -rf "$JDBC_MOD_DIR"

# 3. Copy SQL script
echo "📜 Copying SQL script to JobManager..."
docker exec flink-jobmanager mkdir -p /opt/flink/sql
docker cp "$SQL_FILE" flink-jobmanager:/opt/flink/sql/gaussdb_to_gaussdb.sql

# 4. Restart Clusters
echo "🔄 Restarting Flink containers to apply changes..."
docker restart flink-jobmanager flink-taskmanager

echo "⏳ Waiting for cluster to stabilize (25s)..."
sleep 25

# 5. Initialize GaussDB Sink Table (通过 CN 创建)
echo "🗄️ Initializing GaussDB sink table..."
PGPASSWORD=Gauss_235 psql -h 10.250.0.30 -p 8000 -U tom -d db1 <<EOF
DROP TABLE IF EXISTS products_sink CASCADE;
CREATE TABLE products_sink (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER
);
EOF

# 6. Submit SQL Job
echo "🚀 Submitting SQL job to Flink..."
docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/gaussdb_to_gaussdb.sql

echo ""
echo -e "${GREEN}✅ Success! GaussDB -> GaussDB deployment complete.${NC}"
echo "📝 You can monitor logs with: docker logs -f flink-taskmanager"
echo "🧪 Run tests with: ./run_gaussdb_to_gaussdb_test.sh test"
