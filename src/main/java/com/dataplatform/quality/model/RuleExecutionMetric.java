package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 规则执行监控指标记录 (用于输出到 ClickHouse)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleExecutionMetric implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 规则ID */
    private Long ruleId;
    
    /** 规则名称 */
    private String ruleName;
    
    /** 执行次数 */
    private Long executionCount;
    
    /** 失败次数 */
    private Long failureCount;
    
    /** 异常次数 */
    private Long errorCount;
    
    /** 平均耗时 (毫秒) */
    private Double avgLatencyMs;
    
    /** 失败率 */
    private Double failureRate;
    
    /** 错误率 */
    private Double errorRate;
    
    /** 熔断器状态 */
    private String circuitStatus;
    
    /** 时间戳 */
    private Long timestamp;
    
    /** 规则版本号（用于数据对账和一致性保证）*/
    private Long ruleVersion;
    
    /**
     * 从 RuleMetrics 创建
     */
    public static RuleExecutionMetric fromRuleMetrics(RuleMetrics metrics) {
        return new RuleExecutionMetric(
            metrics.getRuleId(),
            metrics.getRuleName(),
            metrics.getExecutionCount(),
            metrics.getFailureCount(),
            metrics.getErrorCount(),
            metrics.getAvgLatencyMs(),
            metrics.getFailureRate(),
            metrics.getErrorRate(),
            metrics.getCircuitStatus(),
            System.currentTimeMillis(),
            null  // ruleVersion 需要在调用处设置
        );
    }
    
    /**
     * 从 RuleMetrics 创建（带版本号）
     */
    public static RuleExecutionMetric fromRuleMetrics(RuleMetrics metrics, Long ruleVersion) {
        return new RuleExecutionMetric(
            metrics.getRuleId(),
            metrics.getRuleName(),
            metrics.getExecutionCount(),
            metrics.getFailureCount(),
            metrics.getErrorCount(),
            metrics.getAvgLatencyMs(),
            metrics.getFailureRate(),
            metrics.getErrorRate(),
            metrics.getCircuitStatus(),
            System.currentTimeMillis(),
            ruleVersion
        );
    }
}
