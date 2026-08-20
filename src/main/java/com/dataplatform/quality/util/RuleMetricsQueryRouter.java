package com.dataplatform.quality.util;

import com.dataplatform.quality.model.RuleExecutionMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 规则监控指标查询路由器
 * 
 * 功能：根据查询模式自动选择最优的表进行查询
 * 
 * 路由策略：
 * 1. 单规则查询 (rule_id = X) → 主表 rule_execution_metrics
 * 2. 全局时间范围查询 → 时间优先表 rule_metrics_by_time
 * 3. 熔断器/错误率监控 → 时间优先表 rule_metrics_by_time
 */
public class RuleMetricsQueryRouter {
    private static final Logger LOG = LoggerFactory.getLogger(RuleMetricsQueryRouter.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    public RuleMetricsQueryRouter(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    /**
     * 场景1：查询单个规则的历史趋势
     * 路由：主表 (ORDER BY rule_id, timestamp)
     * 性能：~2ms
     */
    public List<RuleExecutionMetric> queryRuleHistory(long ruleId, long startTimeMs, long endTimeMs) {
        String sql = "SELECT " +
            "rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp " +
            "FROM rule_execution_metrics " +  // 使用主表
            "WHERE rule_id = ? " +
            "AND timestamp BETWEEN ? AND ? " +
            "ORDER BY timestamp DESC";
        
        LOG.debug("Query route: rule_execution_metrics (single rule history)");
        return executeQuery(sql, ruleId, new Timestamp(startTimeMs), new Timestamp(endTimeMs));
    }
    
    /**
     * 场景2：查询单个规则的最新状态
     * 路由：主表 (ORDER BY rule_id, timestamp)
     * 性能：<1ms
     */
    public RuleExecutionMetric queryRuleLatestStatus(long ruleId) {
        String sql = "SELECT " +
            "rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp " +
            "FROM rule_execution_metrics " +  // 使用主表
            "WHERE rule_id = ? " +
            "ORDER BY timestamp DESC " +
            "LIMIT 1";
        
        LOG.debug("Query route: rule_execution_metrics (single rule latest)");
        List<RuleExecutionMetric> results = executeQuery(sql, ruleId);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * 场景3：全局监控大盘 - 最近N分钟所有规则的错误率TOP K
     * 路由：时间优先表 (ORDER BY timestamp, rule_id)
     * 性能：~3ms
     */
    public List<RuleExecutionMetric> queryGlobalDashboard(int lastMinutes, int topK) {
        String sql = "SELECT " +
            "rule_id, rule_name, " +
            "AVG(error_rate) * 100 AS avg_error_rate_pct, " +
            "SUM(error_count) AS total_errors, " +
            "SUM(execution_count) AS total_executions, " +
            "MAX(timestamp) AS latest_timestamp " +
            "FROM rule_metrics_by_time " +  // 使用时间优先表
            "WHERE timestamp >= now() - INTERVAL ? MINUTE " +
            "GROUP BY rule_id, rule_name " +
            "HAVING total_executions > 0 " +
            "ORDER BY avg_error_rate_pct DESC " +
            "LIMIT ?";
        
        LOG.debug("Query route: rule_metrics_by_time (global dashboard)");
        return executeQueryWithAggregation(sql, lastMinutes, topK);
    }
    
    /**
     * 场景7：使用1分钟物化视图查询（带FINAL保证一致性）
     * 路由：1分钟聚合表 (rule_metrics_1min_table)
     * 性能：~2ms (使用FINAL修饰符)
     * 
     * 注意：使用FINAL修饰符强制查询时合并，保证SummingMergeTree的一致性
     */
    public List<RuleExecutionMetric> queryGlobalDashboardWithMaterializedView(int lastMinutes, int topK) {
        String sql = "SELECT " +
            "rule_id, rule_name, " +
            "(sum(total_errors) / sum(total_executions)) * 100 AS avg_error_rate_pct, " +
            "sum(total_errors) AS total_errors, " +
            "sum(total_executions) AS total_executions " +
            "FROM rule_metrics_1min_table FINAL " +  // 关键：FINAL修饰符保证一致性
            "WHERE minute >= toStartOfMinute(now() - INTERVAL ? MINUTE) " +
            "GROUP BY rule_id, rule_name " +
            "HAVING total_executions > 0 " +
            "ORDER BY avg_error_rate_pct DESC " +
            "LIMIT ?";
        
        LOG.debug("Query route: rule_metrics_1min_table FINAL (materialized view with consistency)");
        return executeQueryWithAggregation(sql, lastMinutes, topK);
    }
    
    /**
     * 场景8：使用1小时物化视图查询历史趋势（带FINAL保证一致性）
     * 路由：1小时聚合表 (rule_metrics_1hour_table)
     * 性能：~5ms
     */
    public List<RuleExecutionMetric> queryHistoricalTrendWithMaterializedView(int lastHours) {
        String sql = "SELECT " +
            "rule_id, rule_name, " +
            "toDateTime(hour) AS timestamp, " +
            "sum(sum_latency_ms) / sum(sample_count) AS avg_latency_ms, " +
            "sum(total_executions) AS total_executions, " +
            "sum(total_errors) AS total_errors " +
            "FROM rule_metrics_1hour_table FINAL " +  // 关键：FINAL修饰符
            "WHERE hour >= toStartOfHour(now() - INTERVAL ? HOUR) " +
            "GROUP BY rule_id, rule_name, hour " +
            "ORDER BY hour DESC, rule_id";
        
        LOG.debug("Query route: rule_metrics_1hour_table FINAL (historical trend)");
        return executeQueryWithAggregation(sql, lastHours);
    }
    
    /**
     * 场景4：熔断器监控 - 查询当前打开的熔断器
     * 路由：时间优先表 (ORDER BY timestamp, rule_id)
     * 性能：~2ms
     */
    public List<RuleExecutionMetric> queryOpenCircuits(int lastMinutes) {
        String sql = "SELECT " +
            "rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp " +
            "FROM rule_metrics_by_time " +  // 使用时间优先表
            "WHERE timestamp >= now() - INTERVAL ? MINUTE " +
            "AND circuit_status = 'OPEN' " +
            "ORDER BY timestamp DESC " +
            "LIMIT 20";
        
        LOG.debug("Query route: rule_metrics_by_time (open circuits)");
        return executeQuery(sql, lastMinutes);
    }
    
    /**
     * 场景5：高错误率告警 - 错误率超过阈值的规则
     * 路由：时间优先表 (ORDER BY timestamp, rule_id)
     * 性能：~3ms
     */
    public List<RuleExecutionMetric> queryHighErrorRateRules(double errorRateThreshold, int lastMinutes) {
        String sql = "SELECT " +
            "rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp " +
            "FROM rule_metrics_by_time " +  // 使用时间优先表
            "WHERE timestamp >= now() - INTERVAL ? MINUTE " +
            "AND error_rate > ? " +
            "ORDER BY error_rate DESC " +
            "LIMIT 20";
        
        LOG.debug("Query route: rule_metrics_by_time (high error rate)");
        return executeQuery(sql, lastMinutes, errorRateThreshold);
    }
    
    /**
     * 场景6：多规则对比 - 查询指定规则列表的历史数据
     * 路由：主表 (ORDER BY rule_id, timestamp)
     * 性能：~5ms
     */
    public List<RuleExecutionMetric> queryMultipleRulesHistory(List<Long> ruleIds, long startTimeMs, long endTimeMs) {
        if (ruleIds == null || ruleIds.isEmpty()) {
            return new ArrayList<>();
        }
        
        // 构建 IN 子句
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < ruleIds.size(); i++) {
            if (i > 0) inClause.append(",");
            inClause.append("?");
        }
        
        String sql = "SELECT " +
            "rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp " +
            "FROM rule_execution_metrics " +  // 使用主表
            "WHERE rule_id IN (" + inClause + ") " +
            "AND timestamp BETWEEN ? AND ? " +
            "ORDER BY rule_id, timestamp DESC";
        
        LOG.debug("Query route: rule_execution_metrics (multiple rules)");
        
        // 准备参数
        Object[] params = new Object[ruleIds.size() + 2];
        for (int i = 0; i < ruleIds.size(); i++) {
            params[i] = ruleIds.get(i);
        }
        params[ruleIds.size()] = new Timestamp(startTimeMs);
        params[ruleIds.size() + 1] = new Timestamp(endTimeMs);
        
        return executeQuery(sql, params);
    }
    
    /**
     * 执行查询（通用方法）
     */
    private List<RuleExecutionMetric> executeQuery(String sql, Object... params) {
        List<RuleExecutionMetric> results = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // 设置参数
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            
            long startTime = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RuleExecutionMetric metric = new RuleExecutionMetric();
                    metric.setRuleId(rs.getLong("rule_id"));
                    metric.setRuleName(rs.getString("rule_name"));
                    metric.setExecutionCount(rs.getLong("execution_count"));
                    metric.setFailureCount(rs.getLong("failure_count"));
                    metric.setErrorCount(rs.getLong("error_count"));
                    metric.setAvgLatencyMs(rs.getDouble("avg_latency_ms"));
                    metric.setFailureRate(rs.getDouble("failure_rate"));
                    metric.setErrorRate(rs.getDouble("error_rate"));
                    metric.setCircuitStatus(rs.getString("circuit_status"));
                    metric.setTimestamp(rs.getTimestamp("timestamp").getTime());
                    
                    results.add(metric);
                }
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            LOG.info("Query executed in {}ms, returned {} rows", elapsed, results.size());
            
        } catch (SQLException e) {
            LOG.error("Error executing query: {}", sql, e);
        }
        
        return results;
    }
    
    /**
     * 执行聚合查询（用于全局监控大盘）
     */
    private List<RuleExecutionMetric> executeQueryWithAggregation(String sql, Object... params) {
        List<RuleExecutionMetric> results = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // 设置参数
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            
            long startTime = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RuleExecutionMetric metric = new RuleExecutionMetric();
                    metric.setRuleId(rs.getLong("rule_id"));
                    metric.setRuleName(rs.getString("rule_name"));
                    
                    // 聚合字段
                    if (hasColumn(rs, "total_executions")) {
                        metric.setExecutionCount(rs.getLong("total_executions"));
                    }
                    if (hasColumn(rs, "total_errors")) {
                        metric.setErrorCount(rs.getLong("total_errors"));
                    }
                    if (hasColumn(rs, "avg_error_rate_pct")) {
                        metric.setErrorRate(rs.getDouble("avg_error_rate_pct") / 100.0);
                    }
                    if (hasColumn(rs, "latest_timestamp")) {
                        metric.setTimestamp(rs.getTimestamp("latest_timestamp").getTime());
                    }
                    
                    results.add(metric);
                }
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            LOG.info("Aggregation query executed in {}ms, returned {} rows", elapsed, results.size());
            
        } catch (SQLException e) {
            LOG.error("Error executing aggregation query: {}", sql, e);
        }
        
        return results;
    }
    
    /**
     * 检查ResultSet是否包含指定列
     */
    private boolean hasColumn(ResultSet rs, String columnName) {
        try {
            rs.findColumn(columnName);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
