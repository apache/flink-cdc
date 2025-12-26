#!/bin/bash

# ==============================================================================
# GaussDB CDC Sync Result Verification Script
# 验证 GaussDB 和 MySQL 之间的数据一致性
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# GaussDB Connection Details
DB_HOST="10.250.0.51"
DB_PORT="8000"
DB_USER="tom"
DB_PASS="Gauss_235"
DB_NAME="db1"

# 测试数据ID
TEST_ID=999

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  GaussDB CDC 同步结果验证                                  ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ========== 1. 获取 GaussDB 数据 ==========
echo -e "${CYAN}📊 Fetching data from GaussDB...${NC}"
GAUSSDB_DATA=$(PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -A -F'|' -c "SELECT product_id, product_name, category, price, stock FROM products ORDER BY product_id;" 2>/dev/null)
GAUSSDB_COUNT=$(echo "$GAUSSDB_DATA" | grep -v '^$' | wc -l | tr -d ' ')

echo -e "  Total records in GaussDB: ${GREEN}$GAUSSDB_COUNT${NC}"

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

if [ "$GAUSSDB_COUNT" -eq "$MYSQL_COUNT" ]; then
    echo -e "${GREEN}✅ Record count matches: $GAUSSDB_COUNT records${NC}"
    COUNT_MATCH=true
else
    echo -e "${RED}❌ Record count mismatch!${NC}"
    echo -e "   GaussDB: $GAUSSDB_COUNT records"
    echo -e "   MySQL:   $MYSQL_COUNT records"
    echo -e "   Diff:    $((GAUSSDB_COUNT - MYSQL_COUNT)) records"
    COUNT_MATCH=false
fi
echo ""

# ========== 4. 比较数据内容 ==========
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔍 Data Content Comparison${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# 排序并比较数据
GAUSSDB_SORTED=$(echo "$GAUSSDB_DATA" | sort)
MYSQL_SORTED=$(echo "$MYSQL_DATA" | sort)

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

# ========== 5. 检查测试数据 (id=999) ==========
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🧪 Test Data Verification (id=$TEST_ID)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

GAUSSDB_TEST=$(PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -A -F'|' -c "SELECT product_id, product_name, price FROM products WHERE product_id = $TEST_ID;" 2>/dev/null)
MYSQL_TEST=$(docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory --default-character-set=utf8mb4 -se "SELECT product_id, product_name, price FROM products_sink WHERE product_id = $TEST_ID;" 2>/dev/null | tr '\t' '|')

if [ -z "$GAUSSDB_TEST" ] && [ -z "$MYSQL_TEST" ]; then
    echo -e "${GREEN}✅ Test data (id=$TEST_ID) not found in both databases (consistent)${NC}"
    TEST_MATCH=true
elif [ "$GAUSSDB_TEST" == "$MYSQL_TEST" ]; then
    echo -e "${GREEN}✅ Test data (id=$TEST_ID) matches:${NC}"
    echo -e "   $GAUSSDB_TEST"
    TEST_MATCH=true
else
    echo -e "${RED}❌ Test data (id=$TEST_ID) mismatch!${NC}"
    echo -e "   GaussDB: $GAUSSDB_TEST"
    echo -e "   MySQL:   $MYSQL_TEST"
    TEST_MATCH=false
fi
echo ""

# ========== 6. 显示最近的数据 ==========
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}📋 Recent Data (Top 10 records)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo -e "${CYAN}GaussDB (Source):${NC}"
PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT product_id, product_name, category, price, stock FROM products ORDER BY product_id DESC LIMIT 10;" 2>/dev/null | head -15

echo ""
echo -e "${CYAN}MySQL (Sink):${NC}"
docker exec mysql-sink mysql -uflinkuser -pflinkpw inventory --default-character-set=utf8mb4 -e "SELECT product_id, product_name, category, price, stock FROM products_sink ORDER BY product_id DESC LIMIT 10;" 2>/dev/null | head -15

echo ""

# ========== 7. 最终结果 ==========
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  验证结果总结                                              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"

if [ "$COUNT_MATCH" = true ] && [ "$CONTENT_MATCH" = true ] && [ "$TEST_MATCH" = true ]; then
    echo -e "${GREEN}✅ Record Count:  PASSED${NC}"
    echo -e "${GREEN}✅ Data Content:  PASSED${NC}"
    echo -e "${GREEN}✅ Test Data:     PASSED${NC}"
    echo ""
    echo -e "${GREEN}🎉 All checks PASSED! CDC sync is working correctly.${NC}"
    exit 0
else
    [ "$COUNT_MATCH" = true ] && echo -e "${GREEN}✅ Record Count:  PASSED${NC}" || echo -e "${RED}❌ Record Count:  FAILED${NC}"
    [ "$CONTENT_MATCH" = true ] && echo -e "${GREEN}✅ Data Content:  PASSED${NC}" || echo -e "${RED}❌ Data Content:  FAILED${NC}"
    [ "$TEST_MATCH" = true ] && echo -e "${GREEN}✅ Test Data:     PASSED${NC}" || echo -e "${RED}❌ Test Data:     FAILED${NC}"
    echo ""
    echo -e "${RED}❌ Some checks FAILED. CDC sync may have issues.${NC}"
    echo -e "${YELLOW}💡 Tip: Run 'docker logs flink-taskmanager' to see detailed logs${NC}"
    exit 1
fi
