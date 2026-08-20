#!/bin/bash

# ============================================================================
# ClickHouse 性能测试脚本
# 
# 功能:
# 1. 对比原始查询 vs 物化视图查询的性能
# 2. 验证跳数索引的过滤效果
# 3. 检查数据一致性
# ============================================================================

CLICKHOUSE_HOST="192.168.128.141"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_URL="http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/"

echo "=== ClickHouse 性能测试工具 ==="
echo "连接: $CLICKHOUSE_URL"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 执行查询并测量时间
execute_query() {
    local query="$1"
    local description="$2"
    
    echo -e "${YELLOW}测试: $description${NC}"
    
    # 执行查询并测量时间
    local start_time=$(date +%s%N)
    local result=$(curl -s "$CLICKHOUSE_URL" --data "$query")
    local end_time=$(date +%s%N)
    
    # 计算耗时 (毫秒)
    local elapsed_ms=$(( ($end_time - $start_time) / 1000000 ))
    
    echo "$result"
    echo -e "${GREEN}耗时: ${elapsed_ms}ms${NC}"
    echo ""
    
    return $elapsed_ms
}

# 菜单
while true; do
    echo "=== 菜单 ==="
    echo "1. 性能对比: 原始查询 vs 物化视图 (最近5分钟错误率)"
    echo "2. 性能对比: 原始查询 vs 物化视图 (最近24小时趋势)"
    echo "3. 跳数索引测试: Set索引 (熔断器状态)"
    echo "4. 跳数索引测试: MinMax索引 (错误率范围)"
    echo "5. 跳数索引测试: Bloom Filter索引 (规则名称)"
    echo "6. 数据一致性检查"
    echo "7. 查看表统计信息"
    echo "8. 查看索引使用情况"
    echo "9. 完整性能测试报告"
    echo "0. 退出"
    echo ""
    read -p "请选择: " choice
    
    case $choice in
        1)
            echo ""
            echo "=========================================="
            echo "性能对比: 最近5分钟规则错误率TOP 10"
            echo "=========================================="
            echo ""
            
            # 原始查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    (SUM(error_count) / SUM(execution_count)) * 100 AS avg_error_rate_pct,
                    SUM(error_count) AS total_errors,
                    SUM(execution_count) AS total_executions
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 5 MINUTE
                GROUP BY rule_id, rule_name
                HAVING total_executions > 0
                ORDER BY avg_error_rate_pct DESC
                LIMIT 10
                FORMAT PrettyCompact
            " "原始查询 (扫描主表)"
            
            # 物化视图查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    (sum(total_errors) / sum(total_executions)) * 100 AS avg_error_rate_pct,
                    sum(total_errors) AS total_errors,
                    sum(total_executions) AS total_executions
                FROM rule_metrics_1min_table
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
                GROUP BY rule_id, rule_name
                HAVING total_executions > 0
                ORDER BY avg_error_rate_pct DESC
                LIMIT 10
                FORMAT PrettyCompact
            " "物化视图查询 (1分钟粒度)"
            ;;
            
        2)
            echo ""
            echo "=========================================="
            echo "性能对比: 最近24小时规则性能趋势"
            echo "=========================================="
            echo ""
            
            # 原始查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    toStartOfHour(timestamp) AS hour,
                    AVG(avg_latency_ms) AS avg_latency,
                    SUM(execution_count) AS total_executions
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 24 HOUR
                GROUP BY rule_id, rule_name, hour
                ORDER BY hour DESC, avg_latency DESC
                LIMIT 20
                FORMAT PrettyCompact
            " "原始查询 (扫描主表)"
            
            # 物化视图查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    hour,
                    sum(sum_latency_ms) / sum(sample_count) AS avg_latency,
                    sum(total_executions) AS total_executions
                FROM rule_metrics_1hour_table
                WHERE hour >= toStartOfHour(now() - INTERVAL 24 HOUR)
                GROUP BY rule_id, rule_name, hour
                ORDER BY hour DESC, avg_latency DESC
                LIMIT 20
                FORMAT PrettyCompact
            " "物化视图查询 (1小时粒度)"
            ;;
            
        3)
            echo ""
            echo "=========================================="
            echo "跳数索引测试: Set索引 (熔断器状态)"
            echo "=========================================="
            echo ""
            
            # 查看索引使用情况
            echo "索引使用情况:"
            curl -s "$CLICKHOUSE_URL" --data "
                EXPLAIN indexes = 1
                SELECT rule_id, rule_name, error_rate
                FROM rule_execution_metrics
                WHERE circuit_status = 'OPEN'
            "
            echo ""
            echo ""
            
            # 执行查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    error_rate * 100 AS error_rate_pct,
                    circuit_status,
                    timestamp
                FROM rule_execution_metrics
                WHERE circuit_status = 'OPEN'
                ORDER BY timestamp DESC
                LIMIT 20
                FORMAT PrettyCompact
            " "查询熔断器打开的规则"
            ;;
            
        4)
            echo ""
            echo "=========================================="
            echo "跳数索引测试: MinMax索引 (错误率范围)"
            echo "=========================================="
            echo ""
            
            # 查看索引使用情况
            echo "索引使用情况:"
            curl -s "$CLICKHOUSE_URL" --data "
                EXPLAIN indexes = 1
                SELECT rule_id, rule_name, error_rate
                FROM rule_execution_metrics
                WHERE error_rate > 0.3
            "
            echo ""
            echo ""
            
            # 执行查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    error_rate * 100 AS error_rate_pct,
                    error_count,
                    execution_count,
                    timestamp
                FROM rule_execution_metrics
                WHERE error_rate > 0.3
                  AND timestamp >= now() - INTERVAL 10 MINUTE
                ORDER BY error_rate DESC
                LIMIT 20
                FORMAT PrettyCompact
            " "查询错误率>30%的规则"
            ;;
            
        5)
            echo ""
            echo "=========================================="
            echo "跳数索引测试: Bloom Filter索引 (规则名称)"
            echo "=========================================="
            echo ""
            
            # 查看索引使用情况
            echo "索引使用情况:"
            curl -s "$CLICKHOUSE_URL" --data "
                EXPLAIN indexes = 1
                SELECT rule_id, rule_name, error_rate
                FROM rule_execution_metrics
                WHERE rule_name = 'OrderAmountRangeCheck'
            "
            echo ""
            echo ""
            
            # 执行查询
            execute_query "
                SELECT 
                    rule_id,
                    rule_name,
                    execution_count,
                    error_count,
                    error_rate * 100 AS error_rate_pct,
                    avg_latency_ms,
                    circuit_status,
                    timestamp
                FROM rule_execution_metrics
                WHERE rule_name = 'OrderAmountRangeCheck'
                  AND timestamp >= now() - INTERVAL 1 HOUR
                ORDER BY timestamp DESC
                FORMAT PrettyCompact
            " "查询特定规则的最近数据"
            ;;
            
        6)
            echo ""
            echo "=========================================="
            echo "数据一致性检查"
            echo "=========================================="
            echo ""
            
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    'main_table' AS source,
                    sum(execution_count) AS total_executions,
                    sum(error_count) AS total_errors
                FROM rule_execution_metrics
                WHERE timestamp >= toStartOfMinute(now() - INTERVAL 5 MINUTE)

                UNION ALL

                SELECT 
                    'materialized_view' AS source,
                    sum(total_executions) AS total_executions,
                    sum(total_errors) AS total_errors
                FROM rule_metrics_1min_table
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
                
                FORMAT PrettyCompact
            "
            echo ""
            ;;
            
        7)
            echo ""
            echo "=========================================="
            echo "表统计信息"
            echo "=========================================="
            echo ""
            
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    table,
                    formatReadableSize(sum(bytes)) AS size,
                    sum(rows) AS rows,
                    max(modification_time) AS latest_modification
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table LIKE 'rule_%'
                  AND active
                GROUP BY table
                ORDER BY table
                FORMAT PrettyCompact
            "
            echo ""
            ;;
            
        8)
            echo ""
            echo "=========================================="
            echo "索引使用情况"
            echo "=========================================="
            echo ""
            
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    table,
                    name AS index_name,
                    type AS index_type,
                    expr AS index_expression
                FROM system.data_skipping_indices
                WHERE database = currentDatabase()
                  AND table = 'rule_execution_metrics'
                FORMAT PrettyCompact
            "
            echo ""
            ;;
            
        9)
            echo ""
            echo "=========================================="
            echo "完整性能测试报告"
            echo "=========================================="
            echo ""
            
            echo "1. 表大小统计"
            echo "----------------------------------------"
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    table,
                    formatReadableSize(sum(bytes)) AS size,
                    sum(rows) AS rows
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table LIKE 'rule_%'
                  AND active
                GROUP BY table
                ORDER BY table
                FORMAT PrettyCompact
            "
            echo ""
            
            echo "2. 分区信息"
            echo "----------------------------------------"
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    partition,
                    sum(rows) AS rows,
                    formatReadableSize(sum(bytes)) AS size
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table = 'rule_execution_metrics'
                  AND active
                GROUP BY partition
                ORDER BY partition DESC
                LIMIT 10
                FORMAT PrettyCompact
            "
            echo ""
            
            echo "3. 索引列表"
            echo "----------------------------------------"
            curl -s "$CLICKHOUSE_URL" --data "
                SELECT 
                    name AS index_name,
                    type AS index_type,
                    expr AS index_expression
                FROM system.data_skipping_indices
                WHERE database = currentDatabase()
                  AND table = 'rule_execution_metrics'
                FORMAT PrettyCompact
            "
            echo ""
            
            echo "4. 性能测试: 最近5分钟错误率"
            echo "----------------------------------------"
            echo "原始查询:"
            execute_query "
                SELECT count() FROM (
                    SELECT 
                        rule_id,
                        AVG(error_rate) AS avg_error_rate
                    FROM rule_execution_metrics
                    WHERE timestamp >= now() - INTERVAL 5 MINUTE
                    GROUP BY rule_id
                )
            " "原始查询行数统计"
            
            echo "物化视图查询:"
            execute_query "
                SELECT count() FROM (
                    SELECT 
                        rule_id,
                        sum(total_errors) / sum(total_executions) AS error_rate
                    FROM rule_metrics_1min_table
                    WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
                    GROUP BY rule_id
                )
            " "物化视图查询行数统计"
            
            echo "5. 跳数索引过滤效果"
            echo "----------------------------------------"
            echo "Set索引 (circuit_status):"
            curl -s "$CLICKHOUSE_URL" --data "
                EXPLAIN indexes = 1
                SELECT count()
                FROM rule_execution_metrics
                WHERE circuit_status = 'OPEN'
            "
            echo ""
            ;;
            
        0)
            echo "退出程序"
            exit 0
            ;;
            
        *)
            echo "无效选项,请重新选择"
            ;;
    esac
    
    echo ""
    read -p "按回车键继续..."
    echo ""
done
