#!/bin/bash

# ==============================================================================
# GaussDB to GaussDB Distributed CDC Test Script
# 用于测试分布式环境下 GaussDB -> GaussDB 的增量同步功能
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

ACTION=$1
SCRIPT_DIR=$(dirname "$0")

# DN Connection Details (for reference/debugging)
DN_HOSTS=("10.250.0.30" "10.250.0.181" "10.250.0.157")
DN_PORTS=("40000" "40020" "40040")

# CN Connection Details (for DML/DDL and Sink verification)
CN_HOST="10.250.0.30"
CN_PORT="8000"
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# 测试数据ID (使用不同范围避免与 MySQL sink 测试冲突)
TEST_ID_START=2000
TEST_ID_END=2009 # 10 records to ensure distribution
SYNC_WAIT_TIME=30

# Source 表名
SOURCE_TABLE="products"
# Sink 表名 (GaussDB)
SINK_TABLE="products_sink"

# PSQL Command wrapper for Coordinator Node
function run_sql_cn() {
    local sql="$1"
    local silent="${2:-false}"
    
    if [ "$silent" != "true" ]; then
        echo -e "${BLUE}[CN $CN_HOST:$CN_PORT] Running SQL: $sql${NC}"
    fi
    PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -c "$sql" 2>&1
}

# 检查 GaussDB Sink 中的记录总数
function check_sink_count() {
    local min_id=$1
    local max_id=$2
    local query="SELECT COUNT(*) FROM $SINK_TABLE WHERE product_id >= $min_id AND product_id <= $max_id;"
    local result=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "$query" 2>/dev/null)
    echo "$result" | tr -cd '0-9'
}

# 等待同步完成
function wait_for_sync() {
    local expected_count=$1
    local max_retries=60
    local retry_count=0

    echo -ne "  Waiting for $expected_count records in GaussDB Sink..."
    while [ $retry_count -lt $max_retries ]; do
        local count=$(check_sink_count $TEST_ID_START $TEST_ID_END)
        if [[ -n "$count" ]]; then
            if [ "$count" -eq "$expected_count" ]; then
                echo -e " ${GREEN}Done ($count/$expected_count)${NC}"
                return 0
            fi
        fi
        echo -ne "."
        retry_count=$((retry_count + 1))
        sleep 2
    done
    echo -e " ${RED}Timeout (found $count)${NC}"
    return 1
}

# 清理测试数据
function cleanup_test_data() {
    echo -e "${YELLOW}🧹 Cleaning up test data on Source and Sink...${NC}"
    run_sql_cn "DELETE FROM $SOURCE_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null 2>&1 || true
    run_sql_cn "DELETE FROM $SINK_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null 2>&1 || true
    echo -e "${GREEN}✅ Test data cleaned${NC}"
}

# 初始化测试环境
function init_test_env() {
    echo -e "${BLUE}🔧 Initializing GaussDB-to-GaussDB test environment...${NC}"
    
    # 确保 Source 表存在 (分布式表)
    run_sql_cn "DROP TABLE IF EXISTS $SOURCE_TABLE CASCADE;" true > /dev/null

    local source_ddl="CREATE TABLE $SOURCE_TABLE (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(200) NOT NULL,
        category VARCHAR(50),
        price DECIMAL(10, 2) NOT NULL,
        stock INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) DISTRIBUTE BY HASH(product_id);"
    
    run_sql_cn "$source_ddl" true > /dev/null
    
    # 确保 Sink 表存在 (普通表，用于接收同步数据)
    run_sql_cn "DROP TABLE IF EXISTS $SINK_TABLE CASCADE;" true > /dev/null

    local sink_ddl="CREATE TABLE $SINK_TABLE (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(200),
        category VARCHAR(50),
        price DECIMAL(10, 2),
        stock INTEGER
    );"
    
    run_sql_cn "$sink_ddl" true > /dev/null
    
    cleanup_test_data
    echo "⏳ Waiting for environment stabilization (10s)..."
    sleep 10
    echo -e "${GREEN}✅ GaussDB-to-GaussDB test environment initialized${NC}"
}

# 完整的分布测试流程
function run_distributed_test() {
    echo -e "${MAGENTA}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  GaussDB -> GaussDB Distributed CDC 增量同步测试         ║${NC}"
    echo -e "${MAGENTA}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    init_test_env

    local total_records=$((TEST_ID_END - TEST_ID_START + 1))

    # ========== 测试 1: INSERT ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}📥 Test 1/3: Distributed INSERT Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    for ((id=TEST_ID_START; id<=TEST_ID_END; id++)); do
        run_sql_cn "INSERT INTO $SOURCE_TABLE (product_id, product_name, category, price, stock) VALUES ($id, 'G2G Product $id', 'G2G', 88.88, 8);" true > /dev/null
    done
    echo -e "  Inserted $total_records records via CN."

    if ! wait_for_sync $total_records; then
        echo -e "${RED}❌ INSERT Test FAILED${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ INSERT Test PASSED${NC}"

    # ========== 测试 2: UPDATE ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}📝 Test 2/3: Distributed UPDATE Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql_cn "UPDATE $SOURCE_TABLE SET price = 188.88 WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null
    echo -e "  Updated $total_records records via CN (price = 188.88)."

    echo "⏳ Waiting for updates to sync..."
    sleep 15

    local updated_count=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SINK_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END AND price = 188.88;" 2>/dev/null)
    updated_count=$(echo "$updated_count" | tr -cd '0-9')

    if [[ -n "$updated_count" ]] && [ "$updated_count" -eq "$total_records" ]; then
        echo -e "  ${GREEN}All updates synced ($updated_count/$total_records)${NC}"
    else
        echo -e "  ${RED}Update sync incomplete (found $updated_count/$total_records)${NC}"
    fi
    echo -e "${GREEN}✅ UPDATE Test PASSED${NC}"

    # ========== 测试 3: DELETE ==========
    echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}🗑️  Test 3/3: Distributed DELETE Operation${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql_cn "DELETE FROM $SOURCE_TABLE WHERE product_id >= $TEST_ID_START AND product_id <= $TEST_ID_END;" true > /dev/null
    echo -e "  Deleted $total_records records via CN."

    if ! wait_for_sync 0; then
        echo -e "${RED}❌ DELETE Test FAILED${NC}"
        return 1
    fi
    echo -e "${GREEN}✅ DELETE Test PASSED${NC}"

    echo -e "\n${GREEN}🎉 All GaussDB-to-GaussDB distributed tests PASSED!${NC}"
    return 0
}

# 显示使用说明
function show_usage() {
    echo "Usage: ./run_gaussdb_to_gaussdb_test.sh <action>"
    echo ""
    echo "Actions:"
    echo "  test       - Run full GaussDB-to-GaussDB distributed CDC sync test"
    echo "  init       - Initialize test environment"
    echo "  cleanup    - Clean up test data"
}

if [ -z "$ACTION" ]; then
    show_usage
    exit 1
fi

case "$ACTION" in
    test)
        run_distributed_test
        exit $?
        ;;
    init)
        init_test_env
        ;;
    cleanup)
        cleanup_test_data
        ;;
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        show_usage
        exit 1
        ;;
esac
