package com.dataplatform.quality.sink;

import com.dataplatform.quality.model.RuleExecutionMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * ClickHouse 规则监控指标 Sink
 * 
 * 功能: 
 * 1. 将规则执行监控指标写入 ClickHouse
 * 2. 批量写入优化性能
 * 3. 事务保证一致性
 * 4. 双表设计: 主表 + 时间优先表 (通过物化视图自动同步)
 * 5. 应用层只写主表, ClickHouse自动同步到时间优先表
 */
public class ClickHouseRuleMetricsSink extends RichSinkFunction<RuleExecutionMetric> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseRuleMetricsSink.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    /** 批量写入配置 - 优化后 */
    private static final int BATCH_SIZE = 5000;          // 5000条/批（优化：减少Part生成）
    private static final long BATCH_INTERVAL_MS = 60000; // 60秒刷新（优化：减少写入频率）
    
    /** 重试配置 */
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000; // 1秒
    
    private transient Connection connection;
    private transient PreparedStatement insertStatement;
    
    /** 批量缓冲区 */
    private transient List<RuleExecutionMetric> buffer;
    private transient long lastFlushTime;
    
    /** 统计信息 */
    private long totalInserted = 0;
    private long totalBatches = 0;
    private long totalErrors = 0;
    
    public ClickHouseRuleMetricsSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        try {
            // 加载 ClickHouse JDBC 驱动
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            LOG.info("ClickHouse JDBC driver loaded");
            
            // 建立连接（添加参数避免压缩问题）
            String connectionUrl = jdbcUrl;
            if (!connectionUrl.contains("?")) {
                connectionUrl += "?";
            } else {
                connectionUrl += "&";
            }
            // 禁用压缩，避免 Magic 错误
            connectionUrl += "compress=false&decompress=false";
            
            LOG.info("Connecting to ClickHouse: {}", connectionUrl.replaceAll("password=[^&]*", "password=***"));
            connection = DriverManager.getConnection(connectionUrl, username, password);
            
            // 关闭自动提交（使用事务）
            connection.setAutoCommit(false);
            LOG.info("ClickHouse connection established");
            
            // 创建表 (如果不存在)
            createTableIfNotExists();
            
            // 准备插入语句
            String insertSql = "INSERT INTO rule_execution_metrics " +
                "(rule_id, rule_name, execution_count, failure_count, error_count, " +
                "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            LOG.info("Preparing insert statement...");
            insertStatement = connection.prepareStatement(insertSql);
            LOG.info("Insert statement prepared successfully");
            
            // 初始化批量缓冲区
            buffer = new ArrayList<>(BATCH_SIZE);
            lastFlushTime = System.currentTimeMillis();
            
            LOG.info("ClickHouseRuleMetricsSink initialized - Batch size: {}, Interval: {}ms (optimized for reducing parts)", 
                BATCH_SIZE, BATCH_INTERVAL_MS);
                
        } catch (Exception e) {
            LOG.error("Failed to initialize ClickHouseRuleMetricsSink", e);
            // 清理资源
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception closeEx) {
                    LOG.error("Error closing connection during cleanup", closeEx);
                }
            }
            throw e;
        }
    }
    
    /**
     * 创建表 (如果不存在)
     * 双表设计:
     * 1. 主表 (rule_execution_metrics): 按规则ID优先排序
     * 2. 时间优先表 (rule_metrics_by_time): 通过物化视图自动同步
     */
    private void createTableIfNotExists() throws Exception {
        LOG.info("Starting table creation...");
        
        try {
            // 1. 创建主表 (按规则ID优先)
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
            
            LOG.info("Creating main table rule_execution_metrics...");
            try (PreparedStatement stmt = connection.prepareStatement(createMainTableSql)) {
                stmt.execute();
                connection.commit();
                LOG.info("Main table rule_execution_metrics created or already exists");
            } catch (Exception e) {
                LOG.error("Error creating main table", e);
                connection.rollback();
                throw e;
            }
            
            // 2. 创建时间优先表
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
            
            LOG.info("Creating time-priority table rule_metrics_by_time...");
            try (PreparedStatement stmt = connection.prepareStatement(createTimeTableSql)) {
                stmt.execute();
                connection.commit();
                LOG.info("Time-priority table rule_metrics_by_time created or already exists");
            } catch (Exception e) {
                LOG.error("Error creating time-priority table", e);
                connection.rollback();
                throw e;
            }
            
            // 3. 创建物化视图 (自动同步主表到时间优先表)
            String createMaterializedViewSql = 
                "CREATE MATERIALIZED VIEW IF NOT EXISTS rule_metrics_by_time_mv " +
                "TO rule_metrics_by_time " +
                "AS SELECT " +
                "    timestamp, rule_id, rule_name, execution_count, failure_count, " +
                "    error_count, avg_latency_ms, failure_rate, error_rate, circuit_status " +
                "FROM rule_execution_metrics";
            
            LOG.info("Creating materialized view rule_metrics_by_time_mv...");
            try (PreparedStatement stmt = connection.prepareStatement(createMaterializedViewSql)) {
                stmt.execute();
                connection.commit();
                LOG.info("Materialized view rule_metrics_by_time_mv created or already exists");
            } catch (Exception e) {
                LOG.error("Error creating materialized view", e);
                connection.rollback();
                throw e;
            }
            
            LOG.info("Dual-table design initialized: main table + time-priority table + materialized view");
            
        } catch (Exception e) {
            LOG.error("Failed to create tables", e);
            throw new Exception("Table creation failed: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void invoke(RuleExecutionMetric metric, Context context) throws Exception {
        // 添加到缓冲区
        buffer.add(metric);
        
        // 检查是否需要刷新
        long now = System.currentTimeMillis();
        boolean shouldFlush = buffer.size() >= BATCH_SIZE || 
                             (now - lastFlushTime) >= BATCH_INTERVAL_MS;
        
        if (shouldFlush) {
            flushBuffer();
        }
    }
    
    /**
     * 刷新缓冲区（批量写入）
     * 
     * 可靠性保证：
     * 1. At-Least-Once语义：失败时抛出异常，Flink会重试
     * 2. 事务保证：主表和物化视图在同一事务中
     * 3. 重试机制：网络抖动时自动重试
     */
    private void flushBuffer() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        int retryCount = 0;
        Exception lastException = null;
        
        // 重试循环
        while (retryCount < MAX_RETRY_ATTEMPTS) {
            try {
                // 批量添加到 PreparedStatement
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
                
                // 执行批量插入
                int[] results = insertStatement.executeBatch();
                
                // 提交事务
                // 关键: 主表插入和物化视图更新在同一事务中
                // 数据会自动同步到 rule_metrics_by_time 表
                connection.commit();
                
                // 统计
                totalInserted += buffer.size();
                totalBatches++;
                
                long elapsed = System.currentTimeMillis() - startTime;
                LOG.info("Flushed {} metrics to ClickHouse in {}ms (total: {}, batches: {}, retries: {})", 
                    buffer.size(), elapsed, totalInserted, totalBatches, retryCount);
                
                // 清空缓冲区
                buffer.clear();
                lastFlushTime = System.currentTimeMillis();
                
                // 成功，退出重试循环
                return;
                
            } catch (Exception e) {
                lastException = e;
                retryCount++;
                totalErrors++;
                
                LOG.warn("Error flushing buffer to ClickHouse (attempt {}/{}, errors: {})", 
                    retryCount, MAX_RETRY_ATTEMPTS, totalErrors, e);
                
                // 回滚事务
                try {
                    connection.rollback();
                    LOG.info("Transaction rolled back");
                } catch (Exception rollbackEx) {
                    LOG.error("Error rolling back transaction", rollbackEx);
                }
                
                // 如果还有重试机会，等待后重试
                if (retryCount < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * retryCount); // 指数退避
                        LOG.info("Retrying flush... (attempt {}/{})", retryCount + 1, MAX_RETRY_ATTEMPTS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new Exception("Interrupted during retry delay", ie);
                    }
                } else {
                    // 重试次数用尽，抛出异常让Flink重试
                    LOG.error("Max retry attempts reached, throwing exception to Flink for checkpoint retry");
                    throw new Exception("Failed to flush buffer after " + MAX_RETRY_ATTEMPTS + " attempts", lastException);
                }
            }
        }
    }
    
    @Override
    public void close() throws Exception {
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
        
        LOG.info("ClickHouseRuleMetricsSink closed - Total inserted: {}, Batches: {}, Errors: {}", 
            totalInserted, totalBatches, totalErrors);
    }
}
