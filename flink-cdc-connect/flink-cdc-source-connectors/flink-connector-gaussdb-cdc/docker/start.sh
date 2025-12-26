#!/bin/bash
# GaussDB CDC Connector 测试环境启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "GaussDB CDC Connector 测试环境"
echo "=========================================="

# 检查 JAR 文件
JAR_FILE="jars/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "[ERROR] Connector JAR 文件不存在: $JAR_FILE"
    echo ""
    echo "请先构建 JAR 文件:"
    echo "  cd /path/to/flink-cdc"
    echo "  mvn clean package -pl flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc -am -DskipTests"
    echo "  cp flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-gaussdb-cdc/target/flink-sql-connector-gaussdb-cdc-3.6-SNAPSHOT.jar docker/jars/"
    exit 1
fi

echo "[OK] Connector JAR 文件已就绪"

# 创建必要的目录
mkdir -p flink-checkpoints flink-savepoints log

# 启动 Flink 集群
echo ""
echo "[INFO] 启动 Flink 集群..."
docker-compose up -d

# 等待服务启动
echo "[INFO] 等待服务启动..."
sleep 10

# 检查服务状态
echo ""
echo "[INFO] 检查服务状态..."
docker-compose ps

# 检查 JAR 是否正确挂载
echo ""
echo "[INFO] 检查 JAR 文件挂载..."
docker exec flink-jobmanager ls -la /opt/flink/usrlib/

echo ""
echo "=========================================="
echo "Flink 集群已启动!"
echo "=========================================="
echo ""
echo "Web UI: http://localhost:8081"
echo ""
echo "进入 SQL Client:"
echo "  docker exec -it flink-jobmanager ./bin/sql-client.sh"
echo ""
echo "查看日志:"
echo "  docker-compose logs -f"
echo ""
echo "停止集群:"
echo "  docker-compose down"
echo ""
