package com.dataplatform.quality.model;

import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 规则执行监控指标
 * 
 * 功能:
 * 1. 统计每条规则的执行次数、失败次数、异常次数
 * 2. 记录平均耗时
 * 3. 熔断机制：错误率超过阈值自动禁用规则
 */
@Data
public class RuleMetrics implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 规则ID */
    private final Long ruleId;
    
    /** 规则名称 */
    private final String ruleName;
    
    /** 执行次数 */
    private final LongAdder executionCount = new LongAdder();
    
    /** 失败次数 (规则校验不通过) */
    private final LongAdder failureCount = new LongAdder();
    
    /** 异常次数 (规则执行出错) */
    private final LongAdder errorCount = new LongAdder();
    
    /** 总耗时 (纳秒) */
    private final LongAdder totalLatencyNs = new LongAdder();
    
    /** 最后执行时间 */
    private volatile long lastExecutionTime = 0;
    
    /** 熔断器状态 */
    private volatile boolean circuitOpen = false;
    
    /** 熔断器打开时间 */
    private volatile long circuitOpenTime = 0;
    
    /** 熔断配置 */
    private static final double ERROR_RATE_THRESHOLD = 0.5; // 错误率阈值 50%
    private static final long MIN_EXECUTION_COUNT = 100; // 最小执行次数
    private static final long CIRCUIT_RESET_INTERVAL_MS = 60_000; // 熔断恢复时间 1分钟
    
    public RuleMetrics(Long ruleId, String ruleName) {
        this.ruleId = ruleId;
        this.ruleName = ruleName;
    }
    
    /**
     * 记录执行
     */
    public void incrementExecution() {
        executionCount.increment();
        lastExecutionTime = System.currentTimeMillis();
    }
    
    /**
     * 记录失败
     */
    public void incrementFailure() {
        failureCount.increment();
    }
    
    /**
     * 记录异常
     */
    public void incrementError() {
        errorCount.increment();
    }
    
    /**
     * 记录耗时
     */
    public void recordLatency(long latencyNs) {
        totalLatencyNs.add(latencyNs);
    }
    
    /**
     * 获取执行次数
     */
    public long getExecutionCount() {
        return executionCount.sum();
    }
    
    /**
     * 获取失败次数
     */
    public long getFailureCount() {
        return failureCount.sum();
    }
    
    /**
     * 获取异常次数
     */
    public long getErrorCount() {
        return errorCount.sum();
    }
    
    /**
     * 获取平均耗时 (毫秒)
     */
    public double getAvgLatencyMs() {
        long execCount = getExecutionCount();
        if (execCount == 0) {
            return 0.0;
        }
        return totalLatencyNs.sum() / 1_000_000.0 / execCount;
    }
    
    /**
     * 获取失败率
     */
    public double getFailureRate() {
        long execCount = getExecutionCount();
        if (execCount == 0) {
            return 0.0;
        }
        return (double) getFailureCount() / execCount;
    }
    
    /**
     * 获取错误率
     */
    public double getErrorRate() {
        long execCount = getExecutionCount();
        if (execCount == 0) {
            return 0.0;
        }
        return (double) getErrorCount() / execCount;
    }
    
    /**
     * 检查是否应该执行规则 (熔断检查)
     * 
     * @return true=可以执行, false=熔断中
     */
    public boolean shouldExecute() {
        // 如果熔断器打开，检查是否可以恢复
        if (circuitOpen) {
            long now = System.currentTimeMillis();
            if (now - circuitOpenTime >= CIRCUIT_RESET_INTERVAL_MS) {
                // 尝试恢复
                circuitOpen = false;
                circuitOpenTime = 0;
                // 重置计数器
                resetCounters();
                return true;
            }
            return false; // 仍在熔断中
        }
        
        // 检查是否需要熔断
        long execCount = getExecutionCount();
        long errCount = getErrorCount();
        
        if (execCount >= MIN_EXECUTION_COUNT) {
            double errorRate = (double) errCount / execCount;
            if (errorRate > ERROR_RATE_THRESHOLD) {
                // 打开熔断器
                circuitOpen = true;
                circuitOpenTime = System.currentTimeMillis();
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 重置计数器 (熔断恢复时)
     */
    private void resetCounters() {
        executionCount.reset();
        failureCount.reset();
        errorCount.reset();
        totalLatencyNs.reset();
    }
    
    /**
     * 手动打开熔断器
     */
    public void openCircuit() {
        circuitOpen = true;
        circuitOpenTime = System.currentTimeMillis();
    }
    
    /**
     * 手动关闭熔断器
     */
    public void closeCircuit() {
        circuitOpen = false;
        circuitOpenTime = 0;
        resetCounters();
    }
    
    /**
     * 获取熔断器状态字符串
     */
    public String getCircuitStatus() {
        return circuitOpen ? "OPEN" : "CLOSED";
    }
    
    @Override
    public String toString() {
        return String.format(
            "RuleMetrics[ruleId=%d, ruleName=%s, executions=%d, failures=%d, errors=%d, " +
            "failureRate=%.2f%%, errorRate=%.2f%%, avgLatency=%.2fms, circuit=%s]",
            ruleId, ruleName, getExecutionCount(), getFailureCount(), getErrorCount(),
            getFailureRate() * 100, getErrorRate() * 100, getAvgLatencyMs(), getCircuitStatus()
        );
    }
}
