#!/bin/bash

# ==============================================================================
# GaussDB Distributed CDC 一键测试脚本
# 按顺序执行：部署 -> 测试 -> 验证
# ==============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# 脚本路径
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
DEPLOY_SCRIPT="$SCRIPT_DIR/deploy_gaussdb.sh"
TEST_SCRIPT="$SCRIPT_DIR/run_gaussdb_distributed_test.sh"
CHECK_SCRIPT="$SCRIPT_DIR/check_distributed_sync_result.sh"
DIST_SQL_FILE="$SCRIPT_DIR/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker/sql/gaussdb_distributed_to_mysql.sql"

# 测试结果
DEPLOY_SUCCESS=false
TEST_SUCCESS=false
CHECK_SUCCESS=false

# 开始时间
START_TIME=$(date +%s)

# 清屏并显示标题
clear
echo -e "${MAGENTA}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║      GaussDB Distributed CDC 一键测试脚本                        ║"
echo "║      One-Click Distributed Test for GaussDB CDC                  ║"
echo "║                                                                  ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""
echo -e "${CYAN}测试环境 (DNs):${NC}"
echo "  - DN1: 10.250.0.30:40000"
echo "  - DN2: 10.250.0.181:40020"
echo "  - DN3: 10.250.0.157:40040"
echo ""
echo -e "${CYAN}测试流程：${NC}"
echo -e "  1️⃣  使用分布式 SQL 配置部署到 Flink 集群"
echo -e "  2️⃣  对所有 DN 运行增量同步测试 (INSERT/UPDATE/DELETE)"
echo -e "  3️⃣  多节点数据一致性验证"
echo ""
echo -e "${YELLOW}⏱️  开始时间: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo ""

# ========== 步骤 1: 部署 ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 1/3: 部署分布式 CDC 配置                                   ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$DEPLOY_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到部署脚本 $DEPLOY_SCRIPT${NC}"
    exit 1
fi

echo -e "${CYAN}正在执行: SQL_FILE=$DIST_SQL_FILE bash $DEPLOY_SCRIPT${NC}"
echo ""

# 使用环境变量覆盖默认 SQL 文件
if SQL_FILE="$DIST_SQL_FILE" bash "$DEPLOY_SCRIPT"; then
    DEPLOY_SUCCESS=true
    echo ""
    echo -e "${GREEN}✅ 步骤 1 完成: 分布式配置部署成功${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 1 失败: 部署失败${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# 等待集群稳定
echo -e "${YELLOW}⏸️  部署完成，等待 5 秒让集群稳定...${NC}"
sleep 5

# ========== 步骤 2: 运行分布式测试 ==========
echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 2/3: 运行多 DN 增量同步测试                                ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$TEST_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到测试脚本 $TEST_SCRIPT${NC}"
    exit 1
fi

echo -e "${CYAN}正在执行: $TEST_SCRIPT test${NC}"
echo ""

if bash "$TEST_SCRIPT" test; then
    TEST_SUCCESS=true
    echo ""
    echo -e "${GREEN}✅ 步骤 2 完成: 分布式同步测试通过${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 2 失败: 分布式同步测试未通过${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 步骤 3: 验证结果 ==========
echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 3/3: 全局数据一致性验证                                    ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$CHECK_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到验证脚本 $CHECK_SCRIPT${NC}"
    exit 1
fi

echo -e "${CYAN}正在执行: $CHECK_SCRIPT${NC}"
echo ""

if bash "$CHECK_SCRIPT"; then
    CHECK_SUCCESS=true
    echo ""
    echo -e "${GREEN}✅ 步骤 3 完成: 全局验证通过${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 3 失败: 全局验证未通过${NC}"
    exit 1
fi

# ========== 最终报告 ==========
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                      分布式测试完成报告                          ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ⏱️  总耗时: ${MINUTES} 分 ${SECONDS} 秒"
echo -e "  📅 完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [ "$DEPLOY_SUCCESS" = true ] && [ "$TEST_SUCCESS" = true ] && [ "$CHECK_SUCCESS" = true ]; then
    echo -e "${GREEN}🎉 恭喜！GaussDB 分布式 CDC 测试全部通过！${NC}"
    exit 0
else
    echo -e "${RED}❌ 分布式测试失败，请检查各步骤日志。${NC}"
    exit 1
fi
