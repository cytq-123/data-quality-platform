#!/bin/bash

# ============================================================================
# ClickHouse 物化视图一致性验证脚本
# 
# 功能:
# 1. 验证主表和物化视图的数据一致性
# 2. 测试不同查询方式的结果差异
# 3. 演示 FINAL 修饰符的作用
# ============================================================================

CLICKHOUSE_HOST="192.168.128.141"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_URL="http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/"

echo "=== ClickHouse 物化视图一致性验证工具 ==="
echo "连接: $CLICKHOUSE_URL"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 执行查询
execute_query() {
    local query="$1"
    curl -s "$CLICKHOUSE_URL" --data "$query"
}

# 菜单
while true; do
    echo "=== 菜单 ==="
    echo "1. 基础一致性检查 (最近5分钟)"
    echo "2. 详细一致性检查 (按分钟对比)"
    echo "3. 测试 FINAL 修饰符的效果"
    echo "4. 测试合并前后的差异"
    echo "5. 完整一致性报告"
    echo "6. 插入测试数据"
    echo "7. 手动触发合并"
    echo "0. 退出"
    echo ""
    read -p "请选择: " choice
    
    case $choice in
        1)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "基础一致性检查 (最近5分钟)"
            echo -e "==========================================${NC}"
            echo ""
            
            echo -e "${YELLOW}1. 查询主表数据${NC}"
            main_result=$(execute_query "
                SELECT 
                    'main_table' AS source,
                    count() AS record_count,
                    sum(execution_count) AS total_executions,
                    sum(error_count) AS total_errors
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 5 MINUTE
                FORMAT TabSeparated
            ")
            echo "$main_result"
            echo ""
            
            echo -e "${YELLOW}2. 查询物化视图数据 (未合并)${NC}"
            mv_result=$(execute_query "
                SELECT 
                    'materialized_view' AS source,
                    count() AS record_count,
                    sum(total_executions) AS total_executions,
                    sum(total_errors) AS total_errors
                FROM rule_metrics_1min_table
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
                FORMAT TabSeparated
            ")
            echo "$mv_result"
            echo ""
            
            echo -e "${YELLOW}3. 查询物化视图数据 (FINAL 合并)${NC}"
            mv_final_result=$(execute_query "
                SELECT 
                    'materialized_view_final' AS source,
                    count() AS record_count,
                    sum(total_executions) AS total_executions,
                    sum(total_errors) AS total_errors
                FROM rule_metrics_1min_table FINAL
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
                FORMAT TabSeparated
            ")
            echo "$mv_final_result"
            echo ""
            
            # 提取数值进行对比
            main_exec=$(echo "$main_result" | awk '{print $3}')
            mv_final_exec=$(echo "$mv_final_result" | awk '{print $3}')
            
            echo -e "${BLUE}=========================================="
            echo "一致性验证结果"
            echo -e "==========================================${NC}"
            echo "主表总执行次数: $main_exec"
            echo "物化视图总执行次数 (FINAL): $mv_final_exec"
            
            if [ "$main_exec" == "$mv_final_exec" ]; then
                echo -e "${GREEN}✅ 数据一致性验证通过${NC}"
            else
                echo -e "${RED}❌ 数据一致性验证失败${NC}"
                echo "差异: $(($main_exec - $mv_final_exec))"
            fi
            ;;
            
        2)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "详细一致性检查 (按分钟对比)"
            echo -e "==========================================${NC}"
            echo ""
            
            execute_query "
                WITH main_data AS (
                    SELECT 
                        toStartOfMinute(timestamp) AS minute,
                        rule_id,
                        sum(execution_count) AS main_executions,
                        sum(error_count) AS main_errors
                    FROM rule_execution_metrics
                    WHERE timestamp >= now() - INTERVAL 10 MINUTE
                    GROUP BY minute, rule_id
                ),
                mv_data AS (
                    SELECT 
                        minute,
                        rule_id,
                        sum(total_executions) AS mv_executions,
                        sum(total_errors) AS mv_errors
                    FROM rule_metrics_1min_table FINAL
                    WHERE minute >= toStartOfMinute(now() - INTERVAL 10 MINUTE)
                    GROUP BY minute, rule_id
                )
                SELECT 
                    m.minute,
                    m.rule_id,
                    m.main_executions,
                    mv.mv_executions,
                    m.main_executions - mv.mv_executions AS diff_executions,
                    if(m.main_executions = mv.mv_executions, '✅', '❌') AS status
                FROM main_data m
                LEFT JOIN mv_data mv ON m.minute = mv.minute AND m.rule_id = mv.rule_id
                ORDER BY m.minute DESC, m.rule_id
                FORMAT PrettyCompact
            "
            ;;
            
        3)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "测试 FINAL 修饰符的效果"
            echo -e "==========================================${NC}"
            echo ""
            
            echo -e "${YELLOW}场景: 查询同一分钟的数据，对比有无 FINAL 的差异${NC}"
            echo ""
            
            echo -e "${YELLOW}1. 不使用 FINAL (可能看到未合并的数据)${NC}"
            execute_query "
                SELECT 
                    minute,
                    rule_id,
                    total_executions,
                    total_errors
                FROM rule_metrics_1min_table
                WHERE minute = toStartOfMinute(now() - INTERVAL 1 MINUTE)
                ORDER BY rule_id
                FORMAT PrettyCompact
            "
            echo ""
            
            echo -e "${YELLOW}2. 使用 FINAL (强制合并)${NC}"
            execute_query "
                SELECT 
                    minute,
                    rule_id,
                    total_executions,
                    total_errors
                FROM rule_metrics_1min_table FINAL
                WHERE minute = toStartOfMinute(now() - INTERVAL 1 MINUTE)
                ORDER BY rule_id
                FORMAT PrettyCompact
            "
            echo ""
            
            echo -e "${BLUE}说明:${NC}"
            echo "- 不使用 FINAL: 可能看到多行相同 (minute, rule_id) 的数据"
            echo "- 使用 FINAL: 只看到一行，数据已合并"
            ;;
            
        4)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "测试合并前后的差异"
            echo -e "==========================================${NC}"
            echo ""
            
            echo -e "${YELLOW}1. 合并前的数据 (可能有重复)${NC}"
            before_count=$(execute_query "
                SELECT count() 
                FROM rule_metrics_1min_table
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
            ")
            echo "记录数: $before_count"
            echo ""
            
            echo -e "${YELLOW}2. 手动触发合并...${NC}"
            execute_query "OPTIMIZE TABLE rule_metrics_1min_table FINAL"
            echo "合并完成"
            echo ""
            
            sleep 2
            
            echo -e "${YELLOW}3. 合并后的数据${NC}"
            after_count=$(execute_query "
                SELECT count() 
                FROM rule_metrics_1min_table
                WHERE minute >= toStartOfMinute(now() - INTERVAL 5 MINUTE)
            ")
            echo "记录数: $after_count"
            echo ""
            
            echo -e "${BLUE}合并效果:${NC}"
            echo "合并前: $before_count 行"
            echo "合并后: $after_count 行"
            echo "减少: $(($before_count - $after_count)) 行"
            ;;
            
        5)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "完整一致性报告"
            echo -e "==========================================${NC}"
            echo ""
            
            echo -e "${YELLOW}1. 表统计信息${NC}"
            execute_query "
                SELECT 
                    table,
                    sum(rows) AS total_rows,
                    formatReadableSize(sum(bytes)) AS size
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table IN ('rule_execution_metrics', 'rule_metrics_1min_table')
                  AND active
                GROUP BY table
                FORMAT PrettyCompact
            "
            echo ""
            
            echo -e "${YELLOW}2. 最近10分钟数据对比${NC}"
            execute_query "
                SELECT 
                    'main_table' AS source,
                    count() AS records,
                    sum(execution_count) AS total_executions,
                    sum(error_count) AS total_errors
                FROM rule_execution_metrics
                WHERE timestamp >= now() - INTERVAL 10 MINUTE
                
                UNION ALL
                
                SELECT 
                    'materialized_view' AS source,
                    count() AS records,
                    sum(total_executions) AS total_executions,
                    sum(total_errors) AS total_errors
                FROM rule_metrics_1min_table FINAL
                WHERE minute >= toStartOfMinute(now() - INTERVAL 10 MINUTE)
                
                FORMAT PrettyCompact
            "
            echo ""
            
            echo -e "${YELLOW}3. 按规则统计对比${NC}"
            execute_query "
                WITH main_data AS (
                    SELECT 
                        rule_id,
                        rule_name,
                        sum(execution_count) AS main_executions
                    FROM rule_execution_metrics
                    WHERE timestamp >= now() - INTERVAL 10 MINUTE
                    GROUP BY rule_id, rule_name
                ),
                mv_data AS (
                    SELECT 
                        rule_id,
                        rule_name,
                        sum(total_executions) AS mv_executions
                    FROM rule_metrics_1min_table FINAL
                    WHERE minute >= toStartOfMinute(now() - INTERVAL 10 MINUTE)
                    GROUP BY rule_id, rule_name
                )
                SELECT 
                    m.rule_id,
                    m.rule_name,
                    m.main_executions,
                    mv.mv_executions,
                    if(m.main_executions = mv.mv_executions, '✅', '❌') AS consistent
                FROM main_data m
                LEFT JOIN mv_data mv ON m.rule_id = mv.rule_id
                ORDER BY m.rule_id
                FORMAT PrettyCompact
            "
            ;;
            
        6)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "插入测试数据"
            echo -e "==========================================${NC}"
            echo ""
            
            read -p "输入规则ID (默认: 999): " rule_id
            rule_id=${rule_id:-999}
            
            read -p "输入执行次数 (默认: 100): " exec_count
            exec_count=${exec_count:-100}
            
            echo "插入测试数据: rule_id=$rule_id, execution_count=$exec_count"
            
            execute_query "
                INSERT INTO rule_execution_metrics VALUES (
                    $rule_id,
                    'TestRule$rule_id',
                    $exec_count,
                    0,
                    0,
                    5.0,
                    0.0,
                    0.0,
                    'CLOSED',
                    now()
                )
            "
            
            echo -e "${GREEN}✅ 测试数据插入成功${NC}"
            echo ""
            echo "等待2秒后查询..."
            sleep 2
            
            echo "主表数据:"
            execute_query "
                SELECT * FROM rule_execution_metrics
                WHERE rule_id = $rule_id
                ORDER BY timestamp DESC
                LIMIT 5
                FORMAT PrettyCompact
            "
            echo ""
            
            echo "物化视图数据 (FINAL):"
            execute_query "
                SELECT * FROM rule_metrics_1min_table FINAL
                WHERE rule_id = $rule_id
                ORDER BY minute DESC
                LIMIT 5
                FORMAT PrettyCompact
            "
            ;;
            
        7)
            echo ""
            echo -e "${BLUE}=========================================="
            echo "手动触发合并"
            echo -e "==========================================${NC}"
            echo ""
            
            echo "触发主表合并..."
            execute_query "OPTIMIZE TABLE rule_execution_metrics FINAL"
            echo -e "${GREEN}✅ 主表合并完成${NC}"
            echo ""
            
            echo "触发物化视图合并..."
            execute_query "OPTIMIZE TABLE rule_metrics_1min_table FINAL"
            echo -e "${GREEN}✅ 物化视图合并完成${NC}"
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
