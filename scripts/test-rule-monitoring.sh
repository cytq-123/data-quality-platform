#!/bin/bash

# 规则监控测试脚本

echo "=== 规则监控测试工具 ==="
echo ""

CLICKHOUSE_HOST="192.168.128.141"
CLICKHOUSE_PORT="8123"

# 菜单
while true; do
    echo "=== 菜单 ==="
    echo "1. 查看最近5分钟规则错误率"
    echo "2. 查看最近5分钟规则失败率"
    echo "3. 查看最近5分钟规则性能"
    echo "4. 查看熔断器打开的规则"
    echo "5. 查看规则执行趋势 (最近1小时)"
    echo "6. 查看错误率最高的TOP 10规则"
    echo "0. 退出"
    echo ""
    read -p "请选择: " choice
    
    case $choice in
        1)
            echo ""
            echo "=== 最近5分钟规则错误率 ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
                SELECT 
                    rule_id,
                    rule_name,
                    AVG(error_rate) * 100 AS avg_error_rate_pct,
                    MAX(error_rate) * 100 AS max_error_rate_pct,
                    SUM(error_count) AS total_errors,
                    SUM(execution_count) AS total_executions
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 5 MINUTE
                GROUP BY rule_id, rule_name
                HAVING total_executions > 0
                ORDER BY avg_error_rate_pct DESC
                FORMAT PrettyCompact
            "
            ;;
        2)
            echo ""
            echo "=== 最近5分钟规则失败率 ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
                SELECT 
                    rule_id,
                    rule_name,
                    AVG(failure_rate) * 100 AS avg_failure_rate_pct,
                    MAX(failure_rate) * 100 AS max_failure_rate_pct,
                    SUM(failure_count) AS total_failures,
                    SUM(execution_count) AS total_executions
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 5 MINUTE
                GROUP BY rule_id, rule_name
                HAVING total_executions > 0
                ORDER BY avg_failure_rate_pct DESC
                FORMAT PrettyCompact
            "
            ;;
        3)
            echo ""
            echo "=== 最近5分钟规则性能 ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
                SELECT 
                    rule_id,
                    rule_name,
                    AVG(avg_latency_ms) AS avg_latency_ms,
                    MAX(avg_latency_ms) AS max_latency_ms,
                    MIN(avg_latency_ms) AS min_latency_ms
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 5 MINUTE
                GROUP BY rule_id, rule_name
                ORDER BY avg_latency_ms DESC
                FORMAT PrettyCompact
            "
            ;;
        4)
            echo ""
            echo "=== 熔断器打开的规则 ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
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
            "
            ;;
        5)
            echo ""
            echo "=== 规则执行趋势 (最近1小时) ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
                SELECT 
                    rule_id,
                    rule_name,
                    toStartOfMinute(timestamp) AS minute,
                    SUM(execution_count) AS executions,
                    SUM(error_count) AS errors,
                    AVG(error_rate) * 100 AS avg_error_rate_pct
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 1 HOUR
                GROUP BY rule_id, rule_name, minute
                ORDER BY minute DESC, avg_error_rate_pct DESC
                LIMIT 50
                FORMAT PrettyCompact
            "
            ;;
        6)
            echo ""
            echo "=== 错误率最高的TOP 10规则 ==="
            curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" --data "
                SELECT 
                    rule_id,
                    rule_name,
                    AVG(error_rate) * 100 AS avg_error_rate_pct,
                    SUM(error_count) AS total_errors,
                    SUM(execution_count) AS total_executions,
                    circuit_status
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 10 MINUTE
                GROUP BY rule_id, rule_name, circuit_status
                HAVING total_executions > 0
                ORDER BY avg_error_rate_pct DESC
                LIMIT 10
                FORMAT PrettyCompact
            "
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
