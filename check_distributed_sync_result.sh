#!/bin/bash

# ==============================================================================
# GaussDB Distributed CDC Sync Result Verification Script
# 验证多个 GaussDB DN 和 MySQL 之间的数据一致性
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# DN Connection Details
DN_HOSTS=("10.250.0.30" "10.250.0.181" "10.250.0.157")
DN_PORTS=("40000" "40020" "40040")
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# CN Connection Details
CN_HOST="10.250.0.51"
CN_PORT="8000"

# 测试数据ID
TEST_ID_BASE=900

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  GaussDB Distributed CDC 同步结果验证                      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ========== 1. 获取所有 GaussDB DN 的汇总数据 ==========
echo -e "${CYAN}📊 Fetching data from all GaussDB DNs...${NC}"
GAUSSDB_TOTAL_DATA=""
GAUSSDB_TOTAL_COUNT=0

for i in "${!DN_HOSTS[@]}"; do
    host=${DN_HOSTS[$i]}
    port=${DN_PORTS[$i]}
    echo -ne "  -> DN$((i+1)) ($host:$port)... "
    DN_DATA=$(PGPASSWORD=$DB_PASS psql -h $host -p $port -U $DB_USER -d $DB_NAME -t -A -F'|' -c "SELECT product_id, product_name, category, price, stock FROM products;" 2>/dev/null || true)
    DN_COUNT=$(echo "$DN_DATA" | grep -v '^$' | wc -l | tr -d ' ')
    echo -e "${GREEN}$DN_COUNT records${NC}"
    
    if [ -n "$DN_DATA" ]; then
        if [ -n "$GAUSSDB_TOTAL_DATA" ]; then
            GAUSSDB_TOTAL_DATA="${GAUSSDB_TOTAL_DATA}\n${DN_DATA}"
        else
            GAUSSDB_TOTAL_DATA="$DN_DATA"
        fi
    fi
    GAUSSDB_TOTAL_COUNT=$((GAUSSDB_TOTAL_COUNT + DN_COUNT))
done

echo -e "  Total records in GaussDB (all DNs): ${GREEN}$GAUSSDB_TOTAL_COUNT${NC}"

# ========== 2. 获取 MySQL 数据 ==========
echo -e "${CYAN}📊 Fetching data from MySQL...${NC}"
MYSQL_DATA=$(docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory --default-character-set=utf8mb4 -se "SELECT product_id, product_name, category, price, stock FROM products_sink ORDER BY product_id;" 2>/dev/null | tr '\t' '|')
MYSQL_COUNT=$(echo "$MYSQL_DATA" | grep -v '^$' | wc -l | tr -d ' ')

echo -e "  Total records in MySQL: ${GREEN}$MYSQL_COUNT${NC}"
echo ""

# ========== 3. 比较记录数量 ==========
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}📈 Record Count Comparison${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ "$GAUSSDB_TOTAL_COUNT" -eq "$MYSQL_COUNT" ]; then
    echo -e "${GREEN}✅ Record count matches: $GAUSSDB_TOTAL_COUNT records${NC}"
    COUNT_MATCH=true
else
    echo -e "${RED}❌ Record count mismatch!${NC}"
    echo -e "   GaussDB (Total): $GAUSSDB_TOTAL_COUNT records"
    echo -e "   MySQL:           $MYSQL_COUNT records"
    echo -e "   Diff:            $((GAUSSDB_TOTAL_COUNT - MYSQL_COUNT)) records"
    COUNT_MATCH=false
fi
echo ""

# ========== 4. 比较数据内容 ==========
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔍 Data Content Comparison${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# 排序并比较数据
GAUSSDB_SORTED=$(echo -e "$GAUSSDB_TOTAL_DATA" | grep -v '^$' | sort)
MYSQL_SORTED=$(echo "$MYSQL_DATA" | grep -v '^$' | sort)

if [ "$GAUSSDB_SORTED" == "$MYSQL_SORTED" ]; then
    echo -e "${GREEN}✅ Data content matches perfectly!${NC}"
    CONTENT_MATCH=true
else
    echo -e "${RED}❌ Data content mismatch detected!${NC}"
    echo ""
    echo -e "${YELLOW}Differences:${NC}"

    # 显示差异
    diff <(echo "$GAUSSDB_SORTED") <(echo "$MYSQL_SORTED") | head -20 || true

    CONTENT_MATCH=false
fi
echo ""

# ========== 5. 最终结果 ==========
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  验证结果总结                                              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"

if [ "$COUNT_MATCH" = true ] && [ "$CONTENT_MATCH" = true ]; then
    echo -e "${GREEN}✅ Record Count:  PASSED${NC}"
    echo -e "${GREEN}✅ Data Content:  PASSED${NC}"
    echo ""
    echo -e "${GREEN}🎉 All checks PASSED! Distributed CDC sync is working correctly.${NC}"
    exit 0
else
    [ "$COUNT_MATCH" = true ] && echo -e "${GREEN}✅ Record Count:  PASSED${NC}" || echo -e "${RED}❌ Record Count:  FAILED${NC}"
    [ "$CONTENT_MATCH" = true ] && echo -e "${GREEN}✅ Data Content:  PASSED${NC}" || echo -e "${RED}❌ Data Content:  FAILED${NC}"
    echo ""
    echo -e "${RED}❌ Some checks FAILED. Distributed CDC sync may have issues.${NC}"
    exit 1
fi
