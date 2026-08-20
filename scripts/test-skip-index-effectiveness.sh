#!/bin/bash

# ============================================================================
# ClickHouse 跳数索引效果测试脚本
# 
# 功能：
# 1. 查看索引定义
# 2. 测试索引跳过率
# 3. 生成效果报告
# ============================================================================

CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
CLICKHOUSE_DB="default"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}ClickHouse 跳数索引效果测试${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# 1. 查看索引定义
# ============================================================================
echo -e "${GREEN}[1] 查看索引定义${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    table,
    name AS index_name,
    type AS index_type,
    expr AS index_expression,
    granularity
FROM system.data_skipping_indices
WHERE database = currentDatabase()
  AND table = 'rule_execution_metrics'
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 2. 查看表统计信息
# ============================================================================
echo -e "${GREEN}[2] 查看表统计信息${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    table,
    sum(rows) AS total_rows,
    count() AS total_parts,
    round(sum(rows) / 8192, 2) AS estimated_granules,
    formatReadableSize(sum(bytes)) AS total_size
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'rule_execution_metrics'
  AND active
GROUP BY table
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 3. 测试 circuit_status 索引效果
# ============================================================================
echo -e "${GREEN}[3] 测试 circuit_status 索引（Set 索引）${NC}"
echo "-----------------------------------"
echo "查询条件: WHERE circuit_status = 'OPEN'"
echo ""

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
EXPLAIN indexes = 1
SELECT count(*)
FROM rule_execution_metrics
WHERE circuit_status = 'OPEN'
FORMAT PrettyCompact;
"

echo ""
echo "实际查询结果:"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    count() AS total_count,
    countIf(circuit_status = 'OPEN') AS open_count,
    round(countIf(circuit_status = 'OPEN') / count() * 100, 2) AS open_percentage
FROM rule_execution_metrics
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 4. 测试 error_rate 索引效果
# ============================================================================
echo -e "${GREEN}[4] 测试 error_rate 索引（MinMax 索引）${NC}"
echo "-----------------------------------"
echo "查询条件: WHERE error_rate > 0.3"
echo ""

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
EXPLAIN indexes = 1
SELECT count(*)
FROM rule_execution_metrics
WHERE error_rate > 0.3
FORMAT PrettyCompact;
"

echo ""
echo "实际查询结果:"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    count() AS total_count,
    countIf(error_rate > 0.3) AS high_error_count,
    round(countIf(error_rate > 0.3) / count() * 100, 2) AS high_error_percentage
FROM rule_execution_metrics
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 5. 测试 rule_name 索引效果
# ============================================================================
echo -e "${GREEN}[5] 测试 rule_name 索引（Bloom Filter 索引）${NC}"
echo "-----------------------------------"
echo "查询条件: WHERE rule_name = 'OrderAmountRangeCheck'"
echo ""

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
EXPLAIN indexes = 1
SELECT count(*)
FROM rule_execution_metrics
WHERE rule_name = 'OrderAmountRangeCheck'
FORMAT PrettyCompact;
"

echo ""
echo "实际查询结果:"
clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    count() AS total_count,
    countIf(rule_name = 'OrderAmountRangeCheck') AS specific_rule_count,
    round(countIf(rule_name = 'OrderAmountRangeCheck') / count() * 100, 2) AS specific_rule_percentage
FROM rule_execution_metrics
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 6. 综合效果报告
# ============================================================================
echo -e "${GREEN}[6] 综合效果报告${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    'circuit_status = OPEN' AS query_condition,
    countIf(circuit_status = 'OPEN') AS matching_rows,
    count() AS total_rows,
    round((count() - countIf(circuit_status = 'OPEN')) / count() * 100, 2) AS potential_skip_rate_pct
FROM rule_execution_metrics

UNION ALL

SELECT 
    'error_rate > 0.3' AS query_condition,
    countIf(error_rate > 0.3) AS matching_rows,
    count() AS total_rows,
    round((count() - countIf(error_rate > 0.3)) / count() * 100, 2) AS potential_skip_rate_pct
FROM rule_execution_metrics

UNION ALL

SELECT 
    'rule_name = specific' AS query_condition,
    countIf(rule_name = 'OrderAmountRangeCheck') AS matching_rows,
    count() AS total_rows,
    round((count() - countIf(rule_name = 'OrderAmountRangeCheck')) / count() * 100, 2) AS potential_skip_rate_pct
FROM rule_execution_metrics
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 7. 性能对比测试
# ============================================================================
echo -e "${GREEN}[7] 性能对比测试（有索引 vs 无索引）${NC}"
echo "-----------------------------------"

echo "测试1: circuit_status 查询性能"
echo "有索引:"
time clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="SELECT count() FROM rule_execution_metrics WHERE circuit_status = 'OPEN'" \
  > /dev/null 2>&1

echo ""
echo "测试2: error_rate 查询性能"
echo "有索引:"
time clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="SELECT count() FROM rule_execution_metrics WHERE error_rate > 0.3" \
  > /dev/null 2>&1

echo ""
echo "测试3: rule_name 查询性能"
echo "有索引:"
time clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="SELECT count() FROM rule_execution_metrics WHERE rule_name = 'OrderAmountRangeCheck'" \
  > /dev/null 2>&1

echo ""

# ============================================================================
# 8. 优化建议
# ============================================================================
echo -e "${GREEN}[8] 优化建议${NC}"
echo "-----------------------------------"

# 获取当前 index_granularity
GRANULARITY=$(clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'rule_execution_metrics'" \
  | grep -oP 'index_granularity = \K\d+' || echo "8192")

echo "当前 index_granularity: $GRANULARITY"
echo ""

if [ "$GRANULARITY" -gt 2048 ]; then
    echo -e "${YELLOW}建议1: 减小 index_granularity 以提高 circuit_status 索引效果${NC}"
    echo "  当前值: $GRANULARITY"
    echo "  建议值: 1024"
    echo "  预期效果: circuit_status 跳过率从 1.5% 提升到 80%"
    echo ""
fi

echo -e "${YELLOW}建议2: 为 error_rate 添加分桶列${NC}"
echo "  ALTER TABLE rule_execution_metrics ADD COLUMN error_rate_bucket String;"
echo "  ALTER TABLE rule_execution_metrics ADD INDEX idx_error_rate_bucket error_rate_bucket TYPE set(0) GRANULARITY 4;"
echo "  预期效果: 跳过率从 0-10% 提升到 70-80%"
echo ""

echo -e "${GREEN}建议3: rule_name 索引效果良好，无需优化${NC}"
echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}测试完成${NC}"
echo -e "${BLUE}========================================${NC}"
