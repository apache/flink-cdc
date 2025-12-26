#!/bin/bash

# ==============================================================================
# GaussDB CDC 一键测试脚本
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
TEST_SCRIPT="$SCRIPT_DIR/run_gaussdb_test.sh"
CHECK_SCRIPT="$SCRIPT_DIR/check_sync_result.sh"

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
echo "║          GaussDB CDC 一键测试脚本                                ║"
echo "║          One-Click Test for GaussDB CDC Connector                ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""
echo -e "${CYAN}测试流程：${NC}"
echo -e "  1️⃣  部署最新代码到 Flink 集群"
echo -e "  2️⃣  运行增量同步测试 (INSERT/UPDATE/DELETE)"
echo -e "  3️⃣  验证数据一致性"
echo ""
echo -e "${YELLOW}⏱️  开始时间: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo ""
# 自动运行，无需手动确认
# read -p "按 Enter 键开始测试，或 Ctrl+C 取消..."

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 步骤 1: 部署 ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 1/3: 部署最新代码                                          ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$DEPLOY_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到部署脚本 $DEPLOY_SCRIPT${NC}"
    exit 1
fi

echo -e "${CYAN}正在执行: $DEPLOY_SCRIPT${NC}"
echo ""

if bash "$DEPLOY_SCRIPT"; then
    DEPLOY_SUCCESS=true
    echo ""
    echo -e "${GREEN}✅ 步骤 1 完成: 部署成功${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 1 失败: 部署失败${NC}"
    echo -e "${YELLOW}💡 提示: 请检查编译错误或 Docker 容器状态${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# 等待用户确认
echo -e "${YELLOW}⏸️  部署完成，等待 5 秒让集群稳定...${NC}"
sleep 5

# ========== 步骤 2: 运行测试 ==========
echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 2/3: 运行增量同步测试                                      ║${NC}"
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
    echo -e "${GREEN}✅ 步骤 2 完成: 测试通过${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 2 失败: 测试未通过${NC}"
    echo -e "${YELLOW}💡 提示: 请检查 Flink 日志或数据库连接${NC}"
    echo -e "${YELLOW}   查看日志: docker logs flink-taskmanager${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 步骤 3: 验证结果 ==========
echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 3/3: 验证数据一致性                                        ║${NC}"
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
    echo -e "${GREEN}✅ 步骤 3 完成: 验证通过${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 3 失败: 验证未通过${NC}"
    echo -e "${YELLOW}💡 提示: 数据可能未完全同步，请检查 CDC 配置${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 最终报告 ==========
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                      测试完成报告                                ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${CYAN}测试结果:${NC}"
echo ""

if [ "$DEPLOY_SUCCESS" = true ]; then
    echo -e "  ${GREEN}✅ 步骤 1: 部署成功${NC}"
else
    echo -e "  ${RED}❌ 步骤 1: 部署失败${NC}"
fi

if [ "$TEST_SUCCESS" = true ]; then
    echo -e "  ${GREEN}✅ 步骤 2: 测试通过 (INSERT/UPDATE/DELETE)${NC}"
else
    echo -e "  ${RED}❌ 步骤 2: 测试失败${NC}"
fi

if [ "$CHECK_SUCCESS" = true ]; then
    echo -e "  ${GREEN}✅ 步骤 3: 数据一致性验证通过${NC}"
else
    echo -e "  ${RED}❌ 步骤 3: 数据一致性验证失败${NC}"
fi

echo ""
echo -e "${CYAN}测试统计:${NC}"
echo -e "  ⏱️  总耗时: ${MINUTES} 分 ${SECONDS} 秒"
echo -e "  📅 完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [ "$DEPLOY_SUCCESS" = true ] && [ "$TEST_SUCCESS" = true ] && [ "$CHECK_SUCCESS" = true ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                                                                  ║${NC}"
    echo -e "${GREEN}║  🎉 恭喜！所有测试通过！                                         ║${NC}"
    echo -e "${GREEN}║  🎉 Congratulations! All tests passed!                           ║${NC}"
    echo -e "${GREEN}║                                                                  ║${NC}"
    echo -e "${GREEN}║  GaussDB CDC 增量同步功能正常工作                                ║${NC}"
    echo -e "${GREEN}║                                                                  ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║                                                                  ║${NC}"
    echo -e "${RED}║  ❌ 测试失败                                                     ║${NC}"
    echo -e "${RED}║  ❌ Test Failed                                                  ║${NC}"
    echo -e "${RED}║                                                                  ║${NC}"
    echo -e "${RED}║  请检查上面的错误信息                                           ║${NC}"
    echo -e "${RED}║                                                                  ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${YELLOW}💡 故障排查建议:${NC}"
    echo -e "  1. 查看 Flink 日志: ${CYAN}docker logs flink-taskmanager${NC}"
    echo -e "  2. 检查作业状态: ${CYAN}docker exec flink-jobmanager ./bin/flink list${NC}"
    echo -e "  3. 验证数据库连接: ${CYAN}./run_gaussdb_test.sh init${NC}"
    echo -e "  4. 查看详细文档: ${CYAN}cat TEST_SCRIPTS_GUIDE.md${NC}"
    echo ""
    exit 1
fi
