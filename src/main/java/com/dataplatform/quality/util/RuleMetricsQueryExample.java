package com.dataplatform.quality.util;

import com.dataplatform.quality.model.RuleExecutionMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 规则监控指标查询示例
 * 
 * 演示如何使用 RuleMetricsQueryRouter 进行不同场景的查询
 */
public class RuleMetricsQueryExample {
    private static final Logger LOG = LoggerFactory.getLogger(RuleMetricsQueryExample.class);
    
    public static void main(String[] args) {
        // 初始化查询路由器
        String jdbcUrl = "jdbc:clickhouse://localhost:8123/default";
        String username = "default";
        String password = "";
        
        RuleMetricsQueryRouter router = new RuleMetricsQueryRouter(jdbcUrl, username, password);
        
        // ========== 场景1：单规则历史趋势 ==========
        LOG.info("========== 场景1：单规则历史趋势 ==========");
        long ruleId = 123L;
        long startTime = System.currentTimeMillis() - 24 * 60 * 60 * 1000L; // 24小时前
        long endTime = System.currentTimeMillis();
        
        List<RuleExecutionMetric> history = router.queryRuleHistory(ruleId, startTime, endTime);
        LOG.info("规则 {} 的历史记录: {} 条", ruleId, history.size());
        
        // ========== 场景2：单规则最新状态 ==========
        LOG.info("========== 场景2：单规则最新状态 ==========");
        RuleExecutionMetric latestStatus = router.queryRuleLatestStatus(ruleId);
        if (latestStatus != null) {
            LOG.info("规则 {} 最新状态: 错误率={:.2f}%, 熔断状态={}", 
                ruleId, latestStatus.getErrorRate() * 100, latestStatus.getCircuitStatus());
        }
        
        // ========== 场景3：全局监控大盘 ==========
        LOG.info("========== 场景3：全局监控大盘 ==========");
        int lastMinutes = 5;  // 最近5分钟
        int topK = 10;        // TOP 10
        
        List<RuleExecutionMetric> dashboard = router.queryGlobalDashboard(lastMinutes, topK);
        LOG.info("最近 {} 分钟错误率 TOP {}: {} 条", lastMinutes, topK, dashboard.size());
        
        for (int i = 0; i < Math.min(3, dashboard.size()); i++) {
            RuleExecutionMetric metric = dashboard.get(i);
            LOG.info("  {}. 规则 {} ({}): 错误率={:.2f}%, 执行次数={}", 
                i + 1, metric.getRuleId(), metric.getRuleName(), 
                metric.getErrorRate() * 100, metric.getExecutionCount());
        }
        
        // ========== 场景4：熔断器监控 ==========
        LOG.info("========== 场景4：熔断器监控 ==========");
        List<RuleExecutionMetric> openCircuits = router.queryOpenCircuits(10);
        LOG.info("最近10分钟打开的熔断器: {} 个", openCircuits.size());
        
        for (RuleExecutionMetric metric : openCircuits) {
            LOG.info("  规则 {} ({}): 错误率={:.2f}%, 状态={}", 
                metric.getRuleId(), metric.getRuleName(), 
                metric.getErrorRate() * 100, metric.getCircuitStatus());
        }
        
        // ========== 场景5：高错误率告警 ==========
        LOG.info("========== 场景5：高错误率告警 ==========");
        double errorRateThreshold = 0.3;  // 30%
        List<RuleExecutionMetric> highErrorRules = router.queryHighErrorRateRules(errorRateThreshold, 10);
        LOG.info("错误率超过 {:.0f}% 的规则: {} 个", errorRateThreshold * 100, highErrorRules.size());
        
        for (RuleExecutionMetric metric : highErrorRules) {
            LOG.info("  规则 {} ({}): 错误率={:.2f}%", 
                metric.getRuleId(), metric.getRuleName(), metric.getErrorRate() * 100);
        }
        
        // ========== 场景6：多规则对比 ==========
        LOG.info("========== 场景6：多规则对比 ==========");
        List<Long> ruleIds = Arrays.asList(123L, 456L, 789L);
        List<RuleExecutionMetric> multipleRules = router.queryMultipleRulesHistory(
            ruleIds, startTime, endTime);
        LOG.info("规则 {} 的历史记录: {} 条", ruleIds, multipleRules.size());
        
        // 按规则分组统计
        for (Long id : ruleIds) {
            long count = multipleRules.stream()
                .filter(m -> m.getRuleId() == id)
                .count();
            LOG.info("  规则 {}: {} 条记录", id, count);
        }
    }
}
