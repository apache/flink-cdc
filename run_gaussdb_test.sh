#!/bin/bash

# ==============================================================================
# GaussDB CDC Test Script
# 用于测试增量同步功能（INSERT/UPDATE/DELETE）
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ACTION=$1
SCRIPT_DIR=$(dirname "$0")
DOCKER_DIR="$SCRIPT_DIR/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker"

# GaussDB Connection Details
DB_HOST="10.250.0.51"
DB_PORT="8000"
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# 测试数据ID
TEST_ID=999
SYNC_WAIT_TIME=20  # CDC 同步等待时间（秒） - 增加以提高测试稳定性

# PSQL Command wrapper
function run_sql() {
    local sql="$1"
    local silent="${2:-false}"
    if [ "$silent" != "true" ]; then
        echo -e "${BLUE}Running SQL: $sql${NC}"
    fi
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$sql" 2>&1
}

function run_sql_file() {
    local file="$1"
    echo -e "${BLUE}Running SQL file: $file${NC}"
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$file"
}

# MySQL Command wrapper
function run_mysql() {
    local cmd="$1"
    docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory --default-character-set=utf8mb4 -se "$cmd" 2>/dev/null
}

# 检查 MySQL 中的记录数（带重试/轮询）
function wait_for_mysql_count() {
    local expected=$1
    local query="SELECT COUNT(*) FROM products_sink WHERE product_id = $TEST_ID;"
    local max_retries=30
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        local count=$(run_mysql "$query")
        if [ "$count" == "$expected" ]; then
            echo "$count"
            return 0
        fi
        retry_count=$((retry_count + 1))
        sleep 2
    done
    echo "$count"
    return 1
}

# 检查 MySQL 中的特定值（带重试/轮询）
function wait_for_mysql_value() {
    local expected=$1
    local query="SELECT price FROM products_sink WHERE product_id = $TEST_ID;"
    local max_retries=30
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        local val=$(run_mysql "$query")
        if [ "$val" == "$expected" ]; then
            echo "$val"
            return 0
        fi
        retry_count=$((retry_count + 1))
        sleep 2
    done
    echo "$val"
    return 1
}

# 检查测试数据是否存在
function check_test_data_exists() {
    local count=$(run_sql "SELECT COUNT(*) FROM products WHERE product_id = $TEST_ID;" true | grep -E "^\s*[0-9]+\s*$" | tr -d ' ')
    echo "$count"
}

# 清理测试数据
function cleanup_test_data() {
    echo -e "${YELLOW}🧹 Cleaning up test data (id=$TEST_ID)...${NC}"
    run_sql "DELETE FROM products WHERE product_id = $TEST_ID;" true > /dev/null 2>&1 || true
    run_mysql "DELETE FROM products_sink WHERE product_id = $TEST_ID;" > /dev/null 2>&1 || true
    echo -e "${GREEN}✅ Test data cleaned${NC}"
}

# 初始化测试环境
function init_test_env() {
    echo -e "${BLUE}🔧 Initializing test environment...${NC}"

    # 1. 清理可能存在的测试数据
    cleanup_test_data

    # 2. 等待清理同步
    echo "⏳ Waiting for environment stabilization (10s)..."
    sleep 10

    echo -e "${GREEN}✅ Test environment initialized${NC}"
}

# 完整的测试流程
function run_full_test() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  GaussDB CDC 增量同步完整测试 (改进版 - 轮询验证)           ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # 初始化测试环境
    init_test_env

    local test_passed=0
    local test_failed=0

    # ========== 测试 1: INSERT ==========
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📥 Test 1/3: INSERT Operation${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql "INSERT INTO products (product_id, product_name, category, price, stock) VALUES ($TEST_ID, 'Test Product', 'Test', 99.99, 10);"
    echo "⏳ Waiting for CDC sync (polling up to 60s)..."

    # 验证 INSERT (轮询)
    if wait_for_mysql_count 1 > /dev/null; then
        echo -e "${GREEN}✅ INSERT test PASSED - Record synced to MySQL${NC}"
        ((test_passed++))
    else
        echo -e "${RED}❌ INSERT test FAILED - Timeout waiting for record in MySQL${NC}"
        ((test_failed++))
    fi

    # ========== 测试 2: UPDATE ==========
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📝 Test 2/3: UPDATE Operation${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql "UPDATE products SET price = 199.99, product_name = 'Updated Product' WHERE product_id = $TEST_ID;"
    echo "⏳ Waiting for CDC sync (polling up to 60s)..."

    # 验证 UPDATE (轮询)
    if wait_for_mysql_value "199.99" > /dev/null; then
        echo -e "${GREEN}✅ UPDATE test PASSED - Price updated to 199.99${NC}"
        ((test_passed++))
    else
        echo -e "${RED}❌ UPDATE test FAILED - Timeout waiting for price update in MySQL${NC}"
        ((test_failed++))
    fi

    # ========== 测试 3: DELETE ==========
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}🗑️  Test 3/3: DELETE Operation${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    run_sql "DELETE FROM products WHERE product_id = $TEST_ID;"
    echo "⏳ Waiting for CDC sync (polling up to 60s)..."

    # 验证 DELETE (轮询)
    if wait_for_mysql_count 0 > /dev/null; then
        echo -e "${GREEN}✅ DELETE test PASSED - Record deleted from MySQL${NC}"
        ((test_passed++))
    else
        echo -e "${RED}❌ DELETE test FAILED - Timeout waiting for deletion in MySQL${NC}"
        ((test_failed++))
    fi

    # ========== 测试总结 ==========
    echo -e "\n${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  测试总结                                                  ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo -e "Total Tests: 3"
    echo -e "${GREEN}Passed: $test_passed${NC}"
    echo -e "${RED}Failed: $test_failed${NC}"

    if [ $test_failed -eq 0 ]; then
        echo -e "\n${GREEN}🎉 All tests PASSED! CDC sync is working correctly.${NC}"
        return 0
    else
        echo -e "\n${RED}❌ Some tests FAILED. Please check the logs.${NC}"
        echo -e "${YELLOW}💡 Tip: Run 'docker logs flink-taskmanager' to see detailed logs${NC}"
        return 1
    fi
}

# 显示使用说明
function show_usage() {
    echo "Usage: ./run_gaussdb_test.sh <action>"
    echo ""
    echo "Actions:"
    echo "  test       - Run full CDC sync test (INSERT/UPDATE/DELETE)"
    echo "  init       - Initialize test environment (cleanup)"
    echo "  insert     - Insert test record (id=$TEST_ID)"
    echo "  update     - Update test record (id=$TEST_ID)"
    echo "  delete     - Delete test record (id=$TEST_ID)"
    echo "  cleanup    - Clean up test data"
    echo ""
    echo "Examples:"
    echo "  ./run_gaussdb_test.sh test      # Run full test suite"
    echo "  ./run_gaussdb_test.sh insert    # Insert test data"
    echo "  ./run_gaussdb_test.sh cleanup   # Clean up test data"
}

# ========== Main Logic ==========
if [ -z "$ACTION" ]; then
    show_usage
    exit 1
fi

case "$ACTION" in
    test)
        run_full_test
        exit $?
        ;;
    init)
        init_test_env
        ;;
    insert)
        echo -e "${BLUE}📥 Inserting record (id=$TEST_ID)...${NC}"
        run_sql "INSERT INTO products (product_id, product_name, category, price, stock) VALUES ($TEST_ID, 'Test Product', 'Test', 99.99, 10);"
        echo -e "${GREEN}✅ Record inserted${NC}"
        echo -e "${YELLOW}💡 Wait ${SYNC_WAIT_TIME}s for CDC sync, then run: ./check_sync_result.sh${NC}"
        ;;
    update)
        echo -e "${BLUE}📝 Updating record (id=$TEST_ID)...${NC}"
        run_sql "UPDATE products SET price = price + 10, product_name = 'Updated Product' WHERE product_id = $TEST_ID;"
        echo -e "${GREEN}✅ Record updated${NC}"
        echo -e "${YELLOW}💡 Wait ${SYNC_WAIT_TIME}s for CDC sync, then run: ./check_sync_result.sh${NC}"
        ;;
    delete)
        echo -e "${BLUE}🗑️  Deleting record (id=$TEST_ID)...${NC}"
        run_sql "DELETE FROM products WHERE product_id = $TEST_ID;"
        echo -e "${GREEN}✅ Record deleted${NC}"
        echo -e "${YELLOW}💡 Wait ${SYNC_WAIT_TIME}s for CDC sync, then run: ./check_sync_result.sh${NC}"
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
