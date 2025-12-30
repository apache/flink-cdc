#!/bin/bash

# ==============================================================================
# GaussDB to GaussDB Distributed CDC 一键测试脚本
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
DEPLOY_SCRIPT="$SCRIPT_DIR/deploy_gaussdb_to_gaussdb.sh"
TEST_SCRIPT="$SCRIPT_DIR/run_gaussdb_to_gaussdb_test.sh"
DIST_SQL_FILE="$SCRIPT_DIR/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker/sql/gaussdb_simplified_sync.sql"

# 测试结果
DEPLOY_SUCCESS=false
TEST_SUCCESS=false

# 开始时间
START_TIME=$(date +%s)

# 清屏并显示标题
clear
echo -e "${MAGENTA}"
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║      GaussDB -> GaussDB Simplified CDC 一键测试脚本             ║"
echo "║      One-Click GaussDB-to-GaussDB Distributed Test               ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""
echo -e "${CYAN}测试环境:${NC}"
echo "  Source DNs:"
echo "    - DN1: 10.250.0.30:40000"
echo "    - DN2: 10.250.0.181:40020"
echo "    - DN3: 10.250.0.157:40040"
echo "  Sink (CN): 10.250.0.30:8000"
echo ""
echo -e "${CYAN}测试流程：${NC}"
echo -e "  1️⃣  部署 GaussDB -> GaussDB 同步配置到 Flink 集群"
echo -e "  2️⃣  运行增量同步测试 (INSERT/UPDATE/DELETE)"
echo ""
echo -e "${YELLOW}⏱️  开始时间: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo ""

# ========== 步骤 0: 重置 Flink 集群 ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 0: 重置 Flink 集群（确保干净环境）                          ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

DOCKER_COMPOSE_DIR="$SCRIPT_DIR/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc/docker"

if [ -d "$DOCKER_COMPOSE_DIR" ]; then
    echo -e "${CYAN}🔄 正在停止并删除旧的 Flink 集群...${NC}"
    cd "$DOCKER_COMPOSE_DIR"
    docker-compose down --remove-orphans 2>/dev/null || true
    
    # 清理旧的日志和检查点
    echo -e "${CYAN}🧹 清理旧日志和检查点...${NC}"
    rm -rf ./log/* ./flink-checkpoints/* ./flink-savepoints/* 2>/dev/null || true
    
    echo -e "${CYAN}🚀 正在启动全新的 Flink 集群...${NC}"
    docker-compose up -d
    
    echo -e "${YELLOW}⏳ 等待 Flink 集群启动 (20秒)...${NC}"
    sleep 20
    
    cd "$SCRIPT_DIR"
    echo -e "${GREEN}✅ Flink 集群已重置完成${NC}"
else
    echo -e "${YELLOW}⚠️  未找到 docker-compose 目录，跳过集群重置${NC}"
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 步骤 0.5: 清理复制槽 ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 0.5: 清理 GaussDB 历史残留复制槽 (DN 节点)                  ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$TEST_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到测试脚本 $TEST_SCRIPT${NC}"
    exit 1
fi

echo -e "${CYAN}正在执行: bash $TEST_SCRIPT cleanup${NC}"
echo ""
bash "$TEST_SCRIPT" cleanup

echo -e "${GREEN}✅ 步骤 0.5 完成: 复制槽已清理${NC}"
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ========== 步骤 1: 部署 ==========
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 1/3: 部署 GaussDB -> GaussDB CDC 配置                      ║${NC}"
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
    echo -e "${GREEN}✅ 步骤 1 完成: GaussDB -> GaussDB 配置部署成功${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 1 失败: 部署失败${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo -e "${YELLOW}⏸️  正在准备测试环境...${NC}"

# ========== 步骤 2: 运行分布式测试 ==========
echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║  步骤 2/3: 运行 GaussDB -> GaussDB 增量同步测试                  ║${NC}"
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
    echo -e "${GREEN}✅ 步骤 2 完成: GaussDB -> GaussDB 同步测试通过${NC}"
else
    echo ""
    echo -e "${RED}❌ 步骤 2 失败: 同步测试未通过${NC}"
    exit 1
fi

# ========== 最终报告 ==========
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                GaussDB -> GaussDB 测试完成报告                   ║${NC}"
echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ⏱️  总耗时: ${MINUTES} 分 ${SECONDS} 秒"
echo -e "  📅 完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [ "$DEPLOY_SUCCESS" = true ] && [ "$TEST_SUCCESS" = true ]; then
    echo -e "${GREEN}🎉 恭喜！GaussDB -> GaussDB 分布式 CDC 测试全部通过！${NC}"
    exit 0
else
    echo -e "${RED}❌ 测试失败，请检查各步骤日志。${NC}"
    exit 1
fi
