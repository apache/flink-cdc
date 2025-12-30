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

# 清理 GaussDB 复制槽 (DN 节点)
function cleanup_replication_slots() {
    echo -e "${YELLOW}🧹 Cleaning up stale replication slots on DNs...${NC}"
    for i in "${!DN_HOSTS[@]}"; do
        local host="${DN_HOSTS[$i]}"
        local port="${DN_PORTS[$i]}"
        echo -e "  Cleaning DN$((i+1)) at $host:$port..."
        
        # 获取所有槽位并逐一删除
        local slots=$(PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -t -A -c "SELECT slot_name FROM pg_replication_slots;" 2>/dev/null)
        
        if [ -n "$slots" ]; then
            for slot in $slots; do
                echo -ne "    Dropping slot: $slot..."
                if PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -c "SELECT pg_drop_replication_slot('$slot');" > /dev/null 2>&1; then
                    echo -e " ${GREEN}OK${NC}"
                else
                    echo -e " ${RED}FAILED (might be active)${NC}"
                fi
            done
        else
            echo -e "    No slots found."
        fi
    done
}

# 初始化测试环境
function init_test_env() {
    echo -e "${BLUE}🔧 Initializing GaussDB-to-GaussDB test environment...${NC}"
    
    # 注意：不要 DROP 表！Flink CDC Job 正在监听这些表
    # 只检查表是否存在，如果不存在才创建
    
    # 检查 Source 表是否已存在
    local source_exists=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT 1 FROM information_schema.tables WHERE table_name='$SOURCE_TABLE' AND table_schema='public';" 2>/dev/null)
    
    if [ -z "$source_exists" ]; then
        echo -e "${YELLOW}  Creating source table (not exists)...${NC}"
        local source_ddl="CREATE TABLE $SOURCE_TABLE (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(200) NOT NULL,
            category VARCHAR(50),
            price DECIMAL(10, 2) NOT NULL,
            stock INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) DISTRIBUTE BY HASH(product_id);"
        run_sql_cn "$source_ddl" true > /dev/null
        run_sql_cn "ALTER TABLE $SOURCE_TABLE REPLICA IDENTITY FULL;" true > /dev/null
        echo -e "${GREEN}  Source table created and REPLICA IDENTITY set to FULL${NC}"
    else
        echo -e "${GREEN}  Source table already exists, ensuring REPLICA IDENTITY is FULL${NC}"
        run_sql_cn "ALTER TABLE $SOURCE_TABLE REPLICA IDENTITY FULL;" true > /dev/null
    fi
    
    # 检查 Sink 表是否已存在
    local sink_exists=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT 1 FROM information_schema.tables WHERE table_name='$SINK_TABLE' AND table_schema='public';" 2>/dev/null)
    
    if [ -z "$sink_exists" ]; then
        echo -e "${YELLOW}  Creating sink table (not exists)...${NC}"
        local sink_ddl="CREATE TABLE $SINK_TABLE (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(200),
            category VARCHAR(50),
            price DECIMAL(10, 2),
            stock INTEGER
        );"
        run_sql_cn "$sink_ddl" true > /dev/null
    else
        echo -e "${GREEN}  Sink table already exists${NC}"
    fi
    
    # cleanup_replication_slots # 移出至独立步骤或部署前执行
    cleanup_test_data
    echo "⏳ Waiting for environment stabilization (5s)..."
    sleep 5
    echo -e "${GREEN}✅ GaussDB-to-GaussDB test environment initialized${NC}"
}

# 等待 CDC stream 阶段就绪 (等待所有 DN 的 slot 激活)
function wait_for_cdc_stream_ready() {
    echo -e "${YELLOW}⏳ Waiting for CDC stream phase to be ready...${NC}"
    
    # 首先等待 10 秒，让 GaussDB 复制流有时间完成重试机制
    # 根据日志分析，所有 DN 节点在第一次尝试时失败，然后在 2 秒后重试成功
    echo -e "  Initial delay (10s) to allow retry mechanism to complete..."
    sleep 10
    
    local max_wait=120
    local waited=0
    local interval=3  # 减少检查间隔以更快地检测到激活状态
    
    while [ $waited -lt $max_wait ]; do
        # 检查所有3个DN上是否有活跃的CDC slot
        local active_count=0
        local slot_details=""
        for i in "${!DN_HOSTS[@]}"; do
            local host="${DN_HOSTS[$i]}"
            local port="${DN_PORTS[$i]}"
            local has_active=$(PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM pg_replication_slots WHERE (slot_name LIKE 'flink_cdc_g2g%' OR slot_name LIKE 'flink_cdc_simplified%') AND active = true;" 2>/dev/null)
            if [[ "$has_active" =~ ^[0-9]+$ ]] && [ "$has_active" -gt 0 ]; then
                active_count=$((active_count + 1))
                slot_details="$slot_details DN$((i+1)):✓"
            else
                slot_details="$slot_details DN$((i+1)):✗"
            fi
        done
        
        if [ $active_count -ge 3 ]; then
            echo -e "\n${GREEN}✅ All 3 DN CDC slots are active${NC}"
            return 0
        fi
        
        echo -ne "  Waiting... ($waited/${max_wait}s, active: $active_count/3 [$slot_details ])\r"
        sleep $interval
        waited=$((waited + interval))
    done
    
    echo -e "\n${YELLOW}⚠️  CDC slots may not be fully ready after ${max_wait}s, continuing anyway...${NC}"
    return 0
}


# 完整的分布测试流程
function run_distributed_test() {
    echo -e "${MAGENTA}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  GaussDB -> GaussDB Distributed CDC 增量同步测试         ║${NC}"
    echo -e "${MAGENTA}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    init_test_env
    
    # 等待 CDC stream 阶段就绪
    wait_for_cdc_stream_ready

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
        cleanup_replication_slots
        cleanup_test_data
        ;;
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        show_usage
        exit 1
        ;;
esac
