#!/bin/bash

# ==============================================================================
# GaussDB CDC Connector One-Click Deployment Script
# 确保使用最新代码进行部署
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
MYSQL_DRIVER="$PROJECT_ROOT/$CONNECTOR_MODULE/lib/mysql-connector-java.jar"
FLINK_JDBC_CONNECTOR="$PROJECT_ROOT/$CONNECTOR_MODULE/lib/flink-connector-jdbc.jar"
SQL_FILE="${SQL_FILE:-$PROJECT_ROOT/$CONNECTOR_MODULE/docker/sql/gaussdb_to_mysql.sql}"

echo "🚀 Starting GaussDB CDC deployment process..."

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

# 2. Deploy to Flink Cluster
echo "🚚 Distributing JARs to Flink cluster containers..."
for container in flink-jobmanager flink-taskmanager; do
    echo "  -> Deploying to $container..."

    # 先彻底删除所有可能冲突的 JAR 包
    docker exec $container bash -c "rm -f /opt/flink/lib/flink-connector-gaussdb-cdc-*.jar \
                                       /opt/flink/lib/flink-sql-connector-gaussdb-cdc-*.jar \
                                       /opt/flink/lib/gaussdbjdbc.jar \
                                       /opt/flink/lib/mysql-connector-*.jar \
                                       /opt/flink/lib/flink-connector-jdbc*.jar \
                                       /opt/flink/usrlib/*.jar" || true

    # Copy Connector, JDBC Driver, MySQL and Base JDBC to /opt/flink/lib
    docker cp "$CONNECTOR_JAR" $container:/opt/flink/lib/
    docker cp "$JDBC_DRIVER" $container:/opt/flink/lib/
    [ -f "$MYSQL_DRIVER" ] && docker cp "$MYSQL_DRIVER" $container:/opt/flink/lib/
    [ -f "$FLINK_JDBC_CONNECTOR" ] && docker cp "$FLINK_JDBC_CONNECTOR" $container:/opt/flink/lib/
done

# 3. Copy SQL script
echo "📜 Copying SQL script to JobManager..."
docker exec flink-jobmanager mkdir -p /opt/flink/sql
docker cp "$SQL_FILE" flink-jobmanager:/opt/flink/sql/gaussdb_to_mysql.sql

# 4. Restart Clusters
echo "🔄 Restarting Flink containers to apply changes..."
docker restart flink-jobmanager flink-taskmanager

echo "⏳ Waiting for cluster to stabilize (25s)..."
sleep 25

# 5. Initialize MySQL Table
echo "🗄️ Initializing MySQL sink table..."
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory <<EOF
CREATE TABLE IF NOT EXISTS products_sink (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10,2),
    stock INT,
    created_at TIMESTAMP
);
TRUNCATE TABLE products_sink;
EOF

# 6. Submit SQL Job
echo "🚀 Submitting SQL job to Flink..."
docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/gaussdb_to_mysql.sql

echo ""
echo -e "${GREEN}✅ Success! Deployment complete.${NC}"
echo "📝 You can monitor logs with: docker logs -f flink-taskmanager"
echo "🧪 Run tests with: ./run_gaussdb_test.sh test"
echo "📊 Check results with: ./check_sync_result.sh"
