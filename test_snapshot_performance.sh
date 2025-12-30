#!/bin/bash

# ==============================================================================
# GaussDB CDC Snapshot Performance Test
# 测试快照阶段的写入性能 - 10,000 条记录
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR=$(dirname "$0")

# DN Connection Details
DN_HOSTS=("10.250.0.30" "10.250.0.181" "10.250.0.157")
DN_PORTS=("40000" "40020" "40040")

# CN Connection Details
CN_HOST="10.250.0.30"
CN_PORT="8000"
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# 性能测试参数
TOTAL_RECORDS=100000
BATCH_SIZE=1000


SOURCE_TABLE="products"
SINK_TABLE="products_sink"

# PSQL Command wrapper
function run_sql_cn() {
    local sql="$1"
    local silent="${2:-false}"
    
    if [ "$silent" != "true" ]; then
        echo -e "${BLUE}[CN] $sql${NC}"
    fi
    PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -c "$sql" 2>&1
}

# 获取 sink 表记录数
function get_sink_count() {
    PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SINK_TABLE;" 2>/dev/null | tr -cd '0-9'
}

# 取消所有运行中的 Flink Jobs
function cancel_flink_jobs() {
    echo -e "${YELLOW}🛑 Cancelling all running Flink jobs...${NC}"
    
    # 获取所有运行中的 job ID
    local jobs=$(docker exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null | grep "RUNNING" | awk '{print $4}')
    
    if [ -n "$jobs" ]; then
        for job_id in $jobs; do
            echo -e "  Cancelling job: $job_id"
            docker exec flink-jobmanager /opt/flink/bin/flink cancel $job_id > /dev/null 2>&1 || true
        done
        echo -e "  Waiting 5s for jobs to terminate..."
        sleep 5
    else
        echo -e "  No running jobs found"
    fi
}

# 清理现有数据
function cleanup_data() {
    echo -e "${YELLOW}🧹 Cleaning up existing data...${NC}"
    
    # 先取消 Flink jobs，避免竞态条件
    cancel_flink_jobs
    
    run_sql_cn "TRUNCATE TABLE $SOURCE_TABLE;" true > /dev/null 2>&1 || true
    run_sql_cn "TRUNCATE TABLE $SINK_TABLE;" true > /dev/null 2>&1 || true
    echo -e "${GREEN}✅ Data cleaned${NC}"
}


# 清理复制槽
function cleanup_slots() {
    echo -e "${YELLOW}🧹 Cleaning up replication slots on all DNs...${NC}"
    for i in "${!DN_HOSTS[@]}"; do
        local host="${DN_HOSTS[$i]}"
        local port="${DN_PORTS[$i]}"
        
        local slots=$(PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -t -A -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'flink_cdc%';" 2>/dev/null)
        
        for slot in $slots; do
            PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -c "SELECT pg_drop_replication_slot('$slot');" > /dev/null 2>&1 || true
        done
    done
    echo -e "${GREEN}✅ Slots cleaned${NC}"
}

# 插入测试数据 (批量插入优化)
function insert_test_data() {
    echo -e "${CYAN}📊 Inserting $TOTAL_RECORDS test records in batches of $BATCH_SIZE...${NC}"
    
    local start_time=$(date +%s.%N)
    local failed_batches=0
    
    for ((batch_start=1; batch_start<=TOTAL_RECORDS; batch_start+=BATCH_SIZE)); do
        local batch_end=$((batch_start + BATCH_SIZE - 1))
        if [ $batch_end -gt $TOTAL_RECORDS ]; then
            batch_end=$TOTAL_RECORDS
        fi
        
        # 构建批量 INSERT 语句
        local values=""
        for ((id=batch_start; id<=batch_end; id++)); do
            if [ -n "$values" ]; then
                values="$values,"
            fi
            values="$values($id, 'Product $id', 'PERF_TEST', $((id % 1000)).99, $((id % 100)))"
        done
        
        # 执行插入并检查结果
        local result=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -c \
            "INSERT INTO $SOURCE_TABLE (product_id, product_name, category, price, stock) VALUES $values;" 2>&1)
        
        if [[ ! "$result" =~ "INSERT" ]]; then
            echo -e "\n${RED}Failed batch $batch_start-$batch_end: $result${NC}"
            failed_batches=$((failed_batches + 1))
        fi
        
        echo -ne "  Progress: $batch_end/$TOTAL_RECORDS records inserted\r"
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    # 验证实际插入的记录数
    echo ""
    echo -e "${YELLOW}  Verifying inserted records...${NC}"
    local actual_count=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SOURCE_TABLE;" 2>/dev/null)
    
    if [ "$actual_count" -ne "$TOTAL_RECORDS" ]; then
        echo -e "${RED}❌ DATA INTEGRITY ERROR: Expected $TOTAL_RECORDS, but only $actual_count records in source!${NC}"
        echo -e "${RED}  Attempting to find and insert missing records...${NC}"
        
        # 找出缺失的 ID 并重新插入
        local missing=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "
            WITH expected AS (SELECT generate_series(1, $TOTAL_RECORDS) AS id)
            SELECT e.id FROM expected e
            LEFT JOIN $SOURCE_TABLE p ON e.id = p.product_id
            WHERE p.product_id IS NULL;" 2>/dev/null)
        
        for mid in $missing; do
            PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -c \
                "INSERT INTO $SOURCE_TABLE (product_id, product_name, category, price, stock) VALUES ($mid, 'Product $mid', 'PERF_TEST', $((mid % 1000)).99, $((mid % 100)));" > /dev/null 2>&1
        done
        
        # 再次验证
        actual_count=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SOURCE_TABLE;" 2>/dev/null)
        echo -e "${GREEN}  After repair: $actual_count records${NC}"
    fi
    
    local insert_rate=$(echo "scale=2; $actual_count / $duration" | bc)
    echo -e "${GREEN}✅ Verified $actual_count records in source table (${duration}s, ${insert_rate} records/s)${NC}"
    
    if [ "$actual_count" -ne "$TOTAL_RECORDS" ]; then
        echo -e "${RED}❌ CRITICAL: Still missing records after repair!${NC}"
        return 1
    fi
}


# 验证数据分布
function verify_data_distribution() {
    echo -e "${CYAN}📊 Verifying data distribution across DNs...${NC}"
    local total=0
    for i in "${!DN_HOSTS[@]}"; do
        local host="${DN_HOSTS[$i]}"
        local port="${DN_PORTS[$i]}"
        local count=$(PGPASSWORD=$DB_PASS psql -h "$host" -p "$port" -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SOURCE_TABLE;" 2>/dev/null)
        echo -e "  DN$((i+1)) ($host:$port): $count records"
        total=$((total + count))
    done
    echo -e "  ${GREEN}Total: $total records${NC}"
}

# 部署 Flink CDC Job
function deploy_flink_job() {
    echo -e "${CYAN}🚀 Deploying Flink CDC Job...${NC}"
    
    # 使用现有的部署脚本
    SQL_FILE=flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker/sql/gaussdb_simplified_sync.sql \
        bash deploy_gaussdb_to_gaussdb.sh 2>&1 | tail -n 20
    
    echo -e "${GREEN}✅ Flink CDC Job deployed${NC}"
}

# 监控快照同步性能
function monitor_snapshot_sync() {
    echo -e "${MAGENTA}⏱️  Monitoring snapshot sync performance...${NC}"
    
    # 获取源表的实际记录数作为目标
    local source_count=$(PGPASSWORD=$DB_PASS psql -h $CN_HOST -p $CN_PORT -U $DB_USER -d $DB_NAME -t -A -c "SELECT COUNT(*) FROM $SOURCE_TABLE;" 2>/dev/null)
    echo -e "  Source table has: $source_count records"
    
    local start_time=$(date +%s.%N)
    local last_count=0
    local max_wait=1800  # 30 分钟超时 (for 100k records)
    local waited=0
    local check_interval=5

    
    echo -e "  Target: $source_count records (from source)"
    echo ""
    
    while [ $waited -lt $max_wait ]; do
        local current_count=$(get_sink_count)
        
        if [[ -z "$current_count" ]]; then
            current_count=0
        fi

        
        local elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
        local rate=0
        if (( $(echo "$elapsed > 0" | bc -l) )); then
            rate=$(echo "scale=2; $current_count / $elapsed" | bc)
        fi
        
        # 计算进度百分比 (基于源表实际记录数)
        local progress=$((current_count * 100 / source_count))
        
        # 显示进度条
        printf "\r  [%-50s] %3d%% | %d/%d records | %.2f records/s | %.1fs elapsed" \
            "$(printf '#%.0s' $(seq 1 $((progress / 2))))" \
            "$progress" "$current_count" "$source_count" "$rate" "$elapsed"
        
        if [ "$current_count" -ge "$source_count" ]; then
            local end_time=$(date +%s.%N)
            local total_duration=$(echo "$end_time - $start_time" | bc)
            local avg_rate=$(echo "scale=2; $source_count / $total_duration" | bc)
            
            echo ""
            echo ""
            echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
            echo -e "${GREEN}🎉 Snapshot Sync Complete!${NC}"
            echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
            echo -e "  📊 Total Records: $source_count"
            echo -e "  ⏱️  Total Time: ${total_duration}s"
            echo -e "  🚀 ${CYAN}Average Write Rate: ${avg_rate} records/second${NC}"
            
            # 最终数据完整性验证
            echo ""
            echo -e "  ${YELLOW}Verifying data integrity...${NC}"
            local final_sink=$(get_sink_count)
            if [ "$final_sink" -eq "$source_count" ]; then
                echo -e "  ${GREEN}✅ DATA INTEGRITY: 100% (${final_sink}/${source_count})${NC}"
            else
                echo -e "  ${RED}❌ DATA LOSS: ${final_sink}/${source_count} records${NC}"
            fi
            
            echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
            return 0
        fi
        
        last_count=$current_count
        sleep $check_interval
        waited=$((waited + check_interval))
    done
    
    echo ""
    echo -e "${RED}❌ Timeout after ${max_wait}s. Only synced $current_count/$source_count records.${NC}"
    return 1
}


# 主函数
function main() {
    echo -e "${MAGENTA}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║  GaussDB CDC Snapshot Performance Test                     ║${NC}"
    echo -e "${MAGENTA}║  Target: $TOTAL_RECORDS records                                      ║${NC}"
    echo -e "${MAGENTA}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # 步骤 1: 清理数据和槽位
    cleanup_data
    cleanup_slots
    
    # 步骤 2: 插入测试数据
    insert_test_data
    
    # 步骤 3: 验证数据分布
    verify_data_distribution
    
    # 步骤 4: 部署 Flink CDC Job (触发快照)
    deploy_flink_job
    
    # 步骤 5: 等待 10 秒让 CDC 流激活
    echo -e "${YELLOW}⏳ Waiting 10s for CDC streams to activate...${NC}"
    sleep 10
    
    # 步骤 6: 监控快照同步性能
    monitor_snapshot_sync
}

main "$@"
