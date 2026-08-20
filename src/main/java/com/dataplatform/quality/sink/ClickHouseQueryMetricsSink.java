package com.dataplatform.quality.sink;

import com.dataplatform.quality.model.RuleExecutionMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ClickHouse 查询性能监控 Sink
 * 
 * 功能:
 * 1. 写入数据到 ClickHouse
 * 2. 定期执行查询测试，测量查询延迟
 * 3. 将查询延迟作为 Flink Metrics 暴露到 UI
 * 4. 对比不同查询方式的性能（直接查询 vs 物化视图 vs 跳数索引）
 */
public class ClickHouseQueryMetricsSink extends RichSinkFunction<RuleExecutionMetric> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseQueryMetricsSink.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    /** 批量写入配置 */
    private static final int BATCH_SIZE = 5000;
    private static final long BATCH_INTERVAL_MS = 60000;
    
    private transient Connection connection;
    private transient PreparedStatement insertStatement;
    private transient List<RuleExecutionMetric> buffer;
    private transient long lastFlushTime;
    
    /** 查询性能测试 */
    private transient ScheduledExecutorService queryTestExecutor;
    private static final long QUERY_TEST_INTERVAL_SEC = 30; // 每30秒测试一次
    
    /** Flink Metrics - 查询延迟 */
    private transient volatile long directQueryLatencyMs = 0;
    private transient volatile long materializedViewQueryLatencyMs = 0;
    private transient volatile long indexedQueryLatencyMs = 0;
    private transient volatile long timeRangeQueryLatencyMs = 0;
    private transient volatile double querySpeedup = 0.0;
    
    /** Flink Metrics - 计数器 */
    private transient Counter queryTestCount;
    private transient Counter queryErrorCount;
    
    public ClickHouseQueryMetricsSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化连接和表
        initializeConnection();
        
        // 注册 Flink Metrics
        registerMetrics();
        
        // 启动查询性能测试定时任务
        startQueryPerformanceTest();
        
        LOG.info("ClickHouseQueryMetricsSink initialized with query performance monitoring");
    }
    
    private void initializeConnection() throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        
        String connectionUrl = jdbcUrl;
        if (!connectionUrl.contains("?")) {
            connectionUrl += "?";
        } else {
            connectionUrl += "&";
        }
        connectionUrl += "compress=false&decompress=false";
        
        connection = DriverManager.getConnection(connectionUrl, username, password);
        connection.setAutoCommit(false);
        
        // 创建表
        createTablesIfNotExists();
        
        // 准备插入语句
        String insertSql = "INSERT INTO rule_execution_metrics " +
            "(rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        insertStatement = connection.prepareStatement(insertSql);
        buffer = new ArrayList<>(BATCH_SIZE);
        lastFlushTime = System.currentTimeMillis();
    }
    
    private void createTablesIfNotExists() throws Exception {
        // 创建主表
        String createMainTableSql = 
            "CREATE TABLE IF NOT EXISTS rule_execution_metrics (" +
            "    rule_id UInt64," +
            "    rule_name String," +
            "    execution_count UInt64," +
            "    failure_count UInt64," +
            "    error_count UInt64," +
            "    avg_latency_ms Float64," +
            "    failure_rate Float64," +
            "    error_rate Float64," +
            "    circuit_status String," +
            "    timestamp DateTime," +
            "    INDEX idx_circuit_status circuit_status TYPE set(0) GRANULARITY 4," +
            "    INDEX idx_error_rate error_rate TYPE minmax GRANULARITY 4," +
            "    INDEX idx_rule_name rule_name TYPE bloom_filter(0.01) GRANULARITY 4" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMMDD(timestamp) " +
            "ORDER BY (rule_id, timestamp) " +
            "TTL timestamp + INTERVAL 7 DAY " +
            "SETTINGS index_granularity = 8192";
        
        try (PreparedStatement stmt = connection.prepareStatement(createMainTableSql)) {
            stmt.execute();
            connection.commit();
        }
        
        // 创建时间优先表
        String createTimeTableSql = 
            "CREATE TABLE IF NOT EXISTS rule_metrics_by_time (" +
            "    timestamp DateTime," +
            "    rule_id UInt64," +
            "    rule_name String," +
            "    execution_count UInt64," +
            "    failure_count UInt64," +
            "    error_count UInt64," +
            "    avg_latency_ms Float64," +
            "    failure_rate Float64," +
            "    error_rate Float64," +
            "    circuit_status String," +
            "    INDEX idx_circuit_status circuit_status TYPE set(0) GRANULARITY 4," +
            "    INDEX idx_error_rate error_rate TYPE minmax GRANULARITY 4," +
            "    INDEX idx_rule_name rule_name TYPE bloom_filter(0.01) GRANULARITY 4" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMMDD(timestamp) " +
            "ORDER BY (timestamp, rule_id) " +
            "TTL timestamp + INTERVAL 7 DAY " +
            "SETTINGS index_granularity = 8192";
        
        try (PreparedStatement stmt = connection.prepareStatement(createTimeTableSql)) {
            stmt.execute();
            connection.commit();
        }
        
        // 创建物化视图
        String createMaterializedViewSql = 
            "CREATE MATERIALIZED VIEW IF NOT EXISTS rule_metrics_by_time_mv " +
            "TO rule_metrics_by_time " +
            "AS SELECT " +
            "    timestamp, rule_id, rule_name, execution_count, failure_count, " +
            "    error_count, avg_latency_ms, failure_rate, error_rate, circuit_status " +
            "FROM rule_execution_metrics";
        
        try (PreparedStatement stmt = connection.prepareStatement(createMaterializedViewSql)) {
            stmt.execute();
            connection.commit();
        }
    }
    
    /**
     * 注册 Flink Metrics
     */
    private void registerMetrics() {
        // 查询延迟 Gauge
        getRuntimeContext().getMetricGroup()
            .gauge("clickhouse_direct_query_latency_ms", (Gauge<Long>) () -> directQueryLatencyMs);
        
        getRuntimeContext().getMetricGroup()
            .gauge("clickhouse_materialized_view_query_latency_ms", (Gauge<Long>) () -> materializedViewQueryLatencyMs);
        
        getRuntimeContext().getMetricGroup()
            .gauge("clickhouse_indexed_query_latency_ms", (Gauge<Long>) () -> indexedQueryLatencyMs);
        
        getRuntimeContext().getMetricGroup()
            .gauge("clickhouse_time_range_query_latency_ms", (Gauge<Long>) () -> timeRangeQueryLatencyMs);
        
        getRuntimeContext().getMetricGroup()
            .gauge("clickhouse_query_speedup", (Gauge<Double>) () -> querySpeedup);
        
        // 计数器
        queryTestCount = getRuntimeContext().getMetricGroup()
            .counter("clickhouse_query_test_count");
        
        queryErrorCount = getRuntimeContext().getMetricGroup()
            .counter("clickhouse_query_error_count");
    }
    
    /**
     * 启动查询性能测试定时任务
     */
    private void startQueryPerformanceTest() {
        queryTestExecutor = Executors.newSingleThreadScheduledExecutor();
        
        queryTestExecutor.scheduleAtFixedRate(() -> {
            try {
                runQueryPerformanceTest();
                queryTestCount.inc();
            } catch (Exception e) {
                LOG.error("Query performance test failed", e);
                queryErrorCount.inc();
            }
        }, 10, QUERY_TEST_INTERVAL_SEC, TimeUnit.SECONDS); // 10秒后开始，每30秒执行一次
    }
    
    /**
     * 执行查询性能测试
     */
    private void runQueryPerformanceTest() {
        try {
            // 测试1: 直接查询主表（全表扫描聚合）
            // 使用较大的时间范围，确保能查到数据
            String directQuery = "SELECT " +
                "rule_id, " +
                "COUNT(*) as count, " +
                "AVG(error_rate) as avg_error_rate " +
                "FROM rule_execution_metrics " +
                "WHERE timestamp >= now() - INTERVAL 1 DAY " +
                "GROUP BY rule_id";
            
            long start = System.nanoTime();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(directQuery)) {
                while (rs.next()) {
                    // 消费结果
                }
            }
            directQueryLatencyMs = (System.nanoTime() - start) / 1_000_000;
            
            // 测试2: 查询物化视图（时间优先表）
            String mvQuery = "SELECT " +
                "rule_id, " +
                "COUNT(*) as count, " +
                "AVG(error_rate) as avg_error_rate " +
                "FROM rule_metrics_by_time " +
                "WHERE timestamp >= now() - INTERVAL 1 DAY " +
                "GROUP BY rule_id";
            
            start = System.nanoTime();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(mvQuery)) {
                while (rs.next()) {
                    // 消费结果
                }
            }
            materializedViewQueryLatencyMs = (System.nanoTime() - start) / 1_000_000;
            
            // 测试3: 使用跳数索引的查询（改为查询所有状态，确保有结果）
            String indexedQuery = "SELECT " +
                "circuit_status, " +
                "COUNT(*) as count, " +
                "AVG(error_rate) as avg_error_rate " +
                "FROM rule_execution_metrics " +
                "WHERE timestamp >= now() - INTERVAL 5 MINUTE " +
                "GROUP BY circuit_status";
            
            start = System.nanoTime();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(indexedQuery)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 只有在有结果时才更新指标
                if (rowCount > 0) {
                    indexedQueryLatencyMs = (System.nanoTime() - start) / 1_000_000;
                }
            } catch (Exception e) {
                LOG.warn("Indexed query failed: {}", e.getMessage());
            }
            
            // 测试4: 时间范围查询（分区裁剪）- 确保有足够数据
            String timeRangeQuery = "SELECT " +
                "toStartOfMinute(timestamp) as minute, " +
                "COUNT(*) as count, " +
                "AVG(avg_latency_ms) as avg_latency, " +
                "AVG(error_rate) as avg_error_rate " +
                "FROM rule_execution_metrics " +
                "WHERE timestamp >= now() - INTERVAL 10 MINUTE " +
                "GROUP BY minute " +
                "ORDER BY minute DESC";
            
            start = System.nanoTime();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(timeRangeQuery)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 只有在有结果时才更新指标
                if (rowCount > 0) {
                    timeRangeQueryLatencyMs = (System.nanoTime() - start) / 1_000_000;
                }
            } catch (Exception e) {
                LOG.warn("Time range query failed: {}", e.getMessage());
            }
            
            // 计算加速比
            if (materializedViewQueryLatencyMs > 0 && directQueryLatencyMs > 0) {
                querySpeedup = (double) directQueryLatencyMs / materializedViewQueryLatencyMs;
            }
            
            LOG.info("Query Performance Test - Direct: {}ms, MaterializedView: {}ms, Indexed: {}ms, TimeRange: {}ms, Speedup: {:.2f}x",
                directQueryLatencyMs, materializedViewQueryLatencyMs, indexedQueryLatencyMs, 
                timeRangeQueryLatencyMs, querySpeedup);
                
        } catch (Exception e) {
            LOG.error("Error running query performance test", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void invoke(RuleExecutionMetric metric, Context context) throws Exception {
        buffer.add(metric);
        
        long now = System.currentTimeMillis();
        boolean shouldFlush = buffer.size() >= BATCH_SIZE || 
                             (now - lastFlushTime) >= BATCH_INTERVAL_MS;
        
        if (shouldFlush) {
            flushBuffer();
        }
    }
    
    private void flushBuffer() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        
        for (RuleExecutionMetric metric : buffer) {
            insertStatement.setLong(1, metric.getRuleId());
            insertStatement.setString(2, metric.getRuleName());
            insertStatement.setLong(3, metric.getExecutionCount());
            insertStatement.setLong(4, metric.getFailureCount());
            insertStatement.setLong(5, metric.getErrorCount());
            insertStatement.setDouble(6, metric.getAvgLatencyMs());
            insertStatement.setDouble(7, metric.getFailureRate());
            insertStatement.setDouble(8, metric.getErrorRate());
            insertStatement.setString(9, metric.getCircuitStatus());
            insertStatement.setTimestamp(10, new Timestamp(metric.getTimestamp()));
            
            insertStatement.addBatch();
        }
        
        insertStatement.executeBatch();
        connection.commit();
        
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();
    }
    
    @Override
    public void close() throws Exception {
        // 停止查询测试
        if (queryTestExecutor != null) {
            queryTestExecutor.shutdown();
            queryTestExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        
        // 刷新剩余数据
        try {
            flushBuffer();
        } catch (Exception e) {
            LOG.error("Error flushing remaining data", e);
        }
        
        // 关闭资源
        if (insertStatement != null) {
            insertStatement.close();
        }
        
        if (connection != null) {
            connection.close();
        }
        
        LOG.info("ClickHouseQueryMetricsSink closed");
    }
}
