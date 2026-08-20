#!/bin/bash

# ============================================================================
# ClickHouse Part 数量监控脚本
# 
# 功能：
# 1. 监控活跃 Part 数量
# 2. 检查合并任务状态
# 3. 分析写入延迟
# 4. 生成告警
# ============================================================================

CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
CLICKHOUSE_DB="default"
TABLE_NAME="rule_execution_metrics"

# 告警阈值
PART_WARNING_THRESHOLD=200
PART_CRITICAL_THRESHOLD=300
DELAY_WARNING_THRESHOLD=120  # 秒

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}ClickHouse Part 监控报告${NC}"
echo -e "${BLUE}时间: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================================================
# 1. 检查活跃 Part 数量
# ============================================================================
echo -e "${GREEN}[1] 活跃 Part 数量${NC}"
echo "-----------------------------------"

PART_COUNT=$(clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT count() 
FROM system.parts
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
  AND active
" 2>/dev/null)

if [ -z "$PART_COUNT" ]; then
    echo -e "${RED}错误: 无法获取 Part 数量${NC}"
    exit 1
fi

echo "当前活跃 Part 数量: $PART_COUNT"

# 告警判断
if [ "$PART_COUNT" -ge "$PART_CRITICAL_THRESHOLD" ]; then
    echo -e "${RED}🚨 严重告警: Part 数量超过临界值 ($PART_CRITICAL_THRESHOLD)${NC}"
    echo -e "${RED}   建议: 立即检查写入策略和合并配置${NC}"
elif [ "$PART_COUNT" -ge "$PART_WARNING_THRESHOLD" ]; then
    echo -e "${YELLOW}⚠️  警告: Part 数量接近阈值 ($PART_WARNING_THRESHOLD)${NC}"
    echo -e "${YELLOW}   建议: 增大批量大小或调整合并参数${NC}"
else
    echo -e "${GREEN}✅ 正常: Part 数量在安全范围内${NC}"
fi

echo ""

# ============================================================================
# 2. Part 分区分布
# ============================================================================
echo -e "${GREEN}[2] Part 分区分布${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    partition,
    count() AS part_count,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes)) AS total_size,
    min(min_date) AS min_date,
    max(max_date) AS max_date
FROM system.parts
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
  AND active
GROUP BY partition
ORDER BY partition DESC
LIMIT 10
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 3. 合并任务状态
# ============================================================================
echo -e "${GREEN}[3] 后台合并任务${NC}"
echo "-----------------------------------"

MERGE_COUNT=$(clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT count() 
FROM system.merges
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
" 2>/dev/null)

if [ "$MERGE_COUNT" -eq 0 ]; then
    echo "当前无活跃合并任务"
else
    echo "当前活跃合并任务数: $MERGE_COUNT"
    echo ""
    clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
      --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
      --database=$CLICKHOUSE_DB \
      --query="
SELECT 
    elapsed,
    round(progress * 100, 2) AS progress_pct,
    num_parts,
    result_part_name,
    merge_type,
    formatReadableSize(total_size_bytes_compressed) AS total_size
FROM system.merges
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
FORMAT PrettyCompact;
"
fi

echo ""

# ============================================================================
# 4. 写入延迟检查
# ============================================================================
echo -e "${GREEN}[4] 写入延迟检查${NC}"
echo "-----------------------------------"

DELAY_RESULT=$(clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    max(timestamp) AS latest_timestamp,
    now() - max(timestamp) AS delay_seconds
FROM $TABLE_NAME
FORMAT TabSeparated
" 2>/dev/null)

if [ -z "$DELAY_RESULT" ]; then
    echo -e "${YELLOW}警告: 表中无数据或无法查询${NC}"
else
    LATEST_TIMESTAMP=$(echo "$DELAY_RESULT" | cut -f1)
    DELAY_SECONDS=$(echo "$DELAY_RESULT" | cut -f2)
    
    echo "最新数据时间戳: $LATEST_TIMESTAMP"
    echo "写入延迟: $DELAY_SECONDS 秒"
    
    # 告警判断
    if [ "$DELAY_SECONDS" -ge "$DELAY_WARNING_THRESHOLD" ]; then
        echo -e "${YELLOW}⚠️  警告: 写入延迟超过阈值 ($DELAY_WARNING_THRESHOLD 秒)${NC}"
        echo -e "${YELLOW}   建议: 检查 Flink 作业和网络连接${NC}"
    else
        echo -e "${GREEN}✅ 正常: 写入延迟在可接受范围内${NC}"
    fi
fi

echo ""

# ============================================================================
# 5. 表统计信息
# ============================================================================
echo -e "${GREEN}[5] 表统计信息${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes)) AS total_size,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    round(sum(bytes) / sum(rows), 2) AS avg_row_size_bytes,
    count() AS total_parts,
    countIf(active) AS active_parts,
    countIf(NOT active) AS inactive_parts
FROM system.parts
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 6. 写入速度估算
# ============================================================================
echo -e "${GREEN}[6] 写入速度估算（最近1小时）${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    count() AS rows_last_hour,
    round(count() / 3600, 2) AS rows_per_second,
    round(count() / 60, 2) AS rows_per_minute
FROM $TABLE_NAME
WHERE timestamp >= now() - INTERVAL 1 HOUR
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 7. Part 生成速度（最近24小时）
# ============================================================================
echo -e "${GREEN}[7] Part 生成速度（最近24小时）${NC}"
echo "-----------------------------------"

clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT 
    toStartOfHour(modification_time) AS hour,
    count() AS parts_created
FROM system.parts
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
  AND modification_time >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC
LIMIT 24
FORMAT PrettyCompact;
"

echo ""

# ============================================================================
# 8. 优化建议
# ============================================================================
echo -e "${GREEN}[8] 优化建议${NC}"
echo "-----------------------------------"

# 计算每天预计 Part 数
PARTS_PER_HOUR=$(clickhouse-client --host=$CLICKHOUSE_HOST --port=$CLICKHOUSE_PORT \
  --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
  --database=$CLICKHOUSE_DB \
  --query="
SELECT count()
FROM system.parts
WHERE database = '$CLICKHOUSE_DB'
  AND table = '$TABLE_NAME'
  AND modification_time >= now() - INTERVAL 1 HOUR
" 2>/dev/null)

if [ -n "$PARTS_PER_HOUR" ] && [ "$PARTS_PER_HOUR" -gt 0 ]; then
    PARTS_PER_DAY=$((PARTS_PER_HOUR * 24))
    echo "预计每天生成 Part 数: $PARTS_PER_DAY"
    echo ""
    
    if [ "$PARTS_PER_DAY" -gt 500 ]; then
        echo -e "${RED}🚨 严重问题: 每天生成 Part 数过多 ($PARTS_PER_DAY)${NC}"
        echo -e "${RED}   强烈建议:${NC}"
        echo -e "${RED}   1. 增大批量大小到 5000 条${NC}"
        echo -e "${RED}   2. 增加刷新间隔到 60 秒${NC}"
        echo -e "${RED}   3. 调整 ClickHouse 合并参数${NC}"
    elif [ "$PARTS_PER_DAY" -gt 200 ]; then
        echo -e "${YELLOW}⚠️  警告: 每天生成 Part 数较多 ($PARTS_PER_DAY)${NC}"
        echo -e "${YELLOW}   建议:${NC}"
        echo -e "${YELLOW}   1. 增大批量大小到 1000-5000 条${NC}"
        echo -e "${YELLOW}   2. 增加刷新间隔到 30-60 秒${NC}"
    else
        echo -e "${GREEN}✅ 良好: 每天生成 Part 数在合理范围内 ($PARTS_PER_DAY)${NC}"
    fi
fi

echo ""

# ============================================================================
# 9. 紧急处理命令（仅供参考）
# ============================================================================
echo -e "${GREEN}[9] 紧急处理命令（仅供参考）${NC}"
echo "-----------------------------------"

if [ "$PART_COUNT" -ge "$PART_CRITICAL_THRESHOLD" ]; then
    echo -e "${RED}如果 Part 数量过多，可以手动触发合并（慎用）:${NC}"
    echo ""
    echo "# 优化特定分区（推荐）"
    echo "clickhouse-client --query=\"OPTIMIZE TABLE $TABLE_NAME PARTITION '$(date +%Y%m%d)' FINAL;\""
    echo ""
    echo "# 优化整个表（资源消耗大，慎用）"
    echo "clickhouse-client --query=\"OPTIMIZE TABLE $TABLE_NAME FINAL;\""
    echo ""
    echo -e "${YELLOW}注意: OPTIMIZE TABLE 会消耗大量资源，建议在低峰期执行${NC}"
else
    echo "当前 Part 数量正常，无需手动干预"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}监控完成${NC}"
echo -e "${BLUE}========================================${NC}"
