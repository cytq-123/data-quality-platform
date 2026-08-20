package com.dataplatform.quality.monitor;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简化版 RocksDB 监控器
 * 
 * 功能：
 * 1. 监控数据流量（吞吐量、延迟）
 * 2. 定期输出统计信息
 * 3. 不依赖 RocksDB Statistics 对象
 * 
 * 适用场景：
 * - 快速验证 RocksDB 配置是否生效
 * - 监控作业整体性能
 * - 不需要 RocksDB 内部指标的场景
 */
public class SimpleRocksDBMonitor<T> extends RichMapFunction<T, T> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleRocksDBMonitor.class);
    
    /** 处理计数器 */
    private transient Counter processedCounter;
    
    /** 吞吐量指标 */
    private transient Meter throughputMeter;
    
    /** 上一次报告时间 */
    private long lastReportTime;
    
    /** 上一次处理数量 */
    private long lastProcessedCount;
    
    /** 报告间隔（毫秒） */
    private static final long REPORT_INTERVAL_MS = 60_000;  // 60 秒
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        lastReportTime = System.currentTimeMillis();
        lastProcessedCount = 0;
        
        // 注册 Flink Metrics
        processedCounter = getRuntimeContext()
            .getMetricGroup()
            .addGroup("rocksdb_monitor")
            .counter("processed_records");
        
        throughputMeter = getRuntimeContext()
            .getMetricGroup()
            .addGroup("rocksdb_monitor")
            .meter("throughput", new MeterView(60));  // 60 秒窗口
        
        LOG.info("SimpleRocksDBMonitor initialized");
        LOG.info("Performance report will be printed every 60 seconds");
    }
    
    @Override
    public T map(T value) throws Exception {
        // 更新计数器
        processedCounter.inc();
        throughputMeter.markEvent();
        
        // 定期输出性能报告
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastReportTime >= REPORT_INTERVAL_MS) {
            reportPerformance();
            lastReportTime = currentTime;
        }
        
        return value;
    }
    
    /**
     * 输出性能报告
     */
    private void reportPerformance() {
        long currentCount = processedCounter.getCount();
        long processedDelta = currentCount - lastProcessedCount;
        double throughput = processedDelta / (REPORT_INTERVAL_MS / 1000.0);
        
        LOG.info("========== RocksDB Monitor Performance Report ==========");
        LOG.info("Time Window: {} seconds", REPORT_INTERVAL_MS / 1000);
        LOG.info("Total Processed: {} records", currentCount);
        LOG.info("Processed (last 60s): {} records", processedDelta);
        LOG.info("Throughput: {:.2f} records/sec", throughput);
        LOG.info("Throughput (Meter): {:.2f} records/sec", throughputMeter.getRate());
        
        // 性能分析
        analyzePerformance(throughput);
        
        LOG.info("========================================================");
        
        lastProcessedCount = currentCount;
    }
    
    /**
     * 性能分析
     */
    private void analyzePerformance(double throughput) {
        LOG.info("Performance Analysis:");
        
        if (throughput > 10000) {
            LOG.info("  ✅ Excellent throughput (> 10,000 records/sec)");
        } else if (throughput > 5000) {
            LOG.info("  ✅ Good throughput (5,000 - 10,000 records/sec)");
        } else if (throughput > 1000) {
            LOG.info("  ⚠️  Moderate throughput (1,000 - 5,000 records/sec)");
            LOG.info("  💡 Consider: Increase parallelism or optimize RocksDB config");
        } else {
            LOG.warn("  ⚠️  Low throughput (< 1,000 records/sec)");
            LOG.warn("  💡 Suggestions:");
            LOG.warn("     1. Check Kafka lag");
            LOG.warn("     2. Increase parallelism");
            LOG.warn("     3. Optimize RocksDB WriteBuffer size");
            LOG.warn("     4. Check for backpressure");
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("SimpleRocksDBMonitor closed. Total processed: {} records", 
            processedCounter.getCount());
    }
}
