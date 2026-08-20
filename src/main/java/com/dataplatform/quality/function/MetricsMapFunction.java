package com.dataplatform.quality.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * 带 Metrics 的 Map 函数 - 用于监控所有算子的性能
 * 暴露 avgLatencyMs 指标供 Flink UI 显示
 */
public class MetricsMapFunction<T> extends RichMapFunction<T, T> {
    
    private transient Counter recordCount;
    private transient Counter errorCount;
    private transient LatencyTracker latencyTracker;
    
    private final String operatorName;
    
    public MetricsMapFunction(String operatorName) {
        this.operatorName = operatorName;
    }
    
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        
        // 记录处理的记录数
        this.recordCount = metrics.counter(operatorName + "_record_count");
        
        // 记录错误数
        this.errorCount = metrics.counter(operatorName + "_error_count");
        
        // 创建延迟追踪器
        this.latencyTracker = new LatencyTracker(1000);
        
        // 暴露平均延迟作为 Gauge (这样 Flink UI 会显示 avgLatencyMs)
        metrics.gauge("avgLatencyMs", (Gauge<Long>) () -> latencyTracker.getAverageLatency());
    }
    
    @Override
    public T map(T value) throws Exception {
        long startTime = System.nanoTime();
        
        try {
            recordCount.inc();
            return value;
        } catch (Exception e) {
            errorCount.inc();
            throw e;
        } finally {
            // 计算处理时间 (纳秒转毫秒)
            long latencyMs = (System.nanoTime() - startTime) / 1_000_000;
            latencyTracker.recordLatency(latencyMs);
        }
    }
    
    /**
     * 延迟追踪器 - 计算平均延迟
     */
    private static class LatencyTracker {
        private final long[] latencies;
        private int index = 0;
        private long sum = 0;
        private boolean filled = false;
        
        public LatencyTracker(int windowSize) {
            this.latencies = new long[windowSize];
        }
        
        public synchronized void recordLatency(long latencyMs) {
            if (index < latencies.length) {
                latencies[index] = latencyMs;
                sum += latencyMs;
                index++;
                if (index == latencies.length) {
                    filled = true;
                    index = 0;
                }
            } else {
                sum -= latencies[index];
                latencies[index] = latencyMs;
                sum += latencyMs;
                index = (index + 1) % latencies.length;
            }
        }
        
        public synchronized long getAverageLatency() {
            if (index == 0 && !filled) {
                return 0;
            }
            int count = filled ? latencies.length : index;
            return count > 0 ? sum / count : 0;
        }
    }
}
