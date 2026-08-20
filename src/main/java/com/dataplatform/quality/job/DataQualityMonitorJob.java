package com.dataplatform.quality.job;

import com.dataplatform.quality.config.AdvancedRocksDBConfig;
import com.dataplatform.quality.function.DeduplicationProcessFunction;
import com.dataplatform.quality.function.MetricsMapFunction;
import com.dataplatform.quality.function.QualityMetricsAggregator;
import com.dataplatform.quality.function.RuleEngineProcessFunction;
import com.dataplatform.quality.model.Order;
import com.dataplatform.quality.model.QualityMetrics;
import com.dataplatform.quality.model.ValidationResult;
import com.dataplatform.quality.monitor.SimpleRocksDBMonitor;
import com.dataplatform.quality.sink.ClickHouseSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/**
 * 数据质量监控 Flink 作业
 * 
 * 架构:
 * Kafka Source → 规则引擎校验 → 正常数据/异常数据分流 → Kafka Sink
 *                              → 质量指标聚合 → ClickHouse Sink
 */
public class DataQualityMonitorJob {
    private static final Logger LOG = LoggerFactory.getLogger(DataQualityMonitorJob.class);
    
    public static void main(String[] args) throws Exception {
        // 1. 加载配置
        Properties config = loadConfig();
        
        // 2. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // ========== RocksDB 高级配置（源码级优化） ==========
        
        // 配置 RocksDB 状态后端（支持亿级状态存储）
        EmbeddedRocksDBStateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend(true);  // true = 启用增量 Checkpoint
        
        // 使用高级 RocksDB 配置（带性能监控和自适应调优）
        // 选择 LARGE_STATE 模式：针对 60 GB 去重状态优化
        AdvancedRocksDBConfig rocksDBConfig = new AdvancedRocksDBConfig(
            AdvancedRocksDBConfig.ConfigMode.LARGE_STATE,  // 大状态模式
            true  // 启用详细日志（用于性能分析）
        );
        rocksDBBackend.setRocksDBOptions(rocksDBConfig);
        
        env.setStateBackend(rocksDBBackend);
        
        LOG.info("========== RocksDB Configuration ==========");
        LOG.info("Mode: LARGE_STATE (optimized for 60 GB deduplication state)");
        LOG.info("Incremental Checkpoint: ENABLED");
        LOG.info("Performance Monitoring: ENABLED");
        LOG.info("Expected State Size: ~60 GB for 1 billion orders");
        LOG.info("==========================================");
        
        // 配置 Checkpoint（每 5 分钟，适合大状态）
        env.enableCheckpointing(300000);  // 5 分钟
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);  // 最小间隔 1 分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000);  // 超时 10 分钟
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);  // 最多 1 个并发 Checkpoint
        
        // 配置 Checkpoint 存储（生产环境使用 HDFS 或 S3）
        String checkpointPath = config.getProperty("flink.checkpoint.path", "file:///tmp/flink-checkpoints");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        
        // 配置并行度（增加到 32，大幅提升处理能力）
        env.setParallelism(Integer.parseInt(config.getProperty("flink.parallelism", "32")));
        
        LOG.info("Checkpoint path: {}", checkpointPath);
        LOG.info("Checkpoint interval: 5 minutes");
        LOG.info("Parallelism: {}", env.getParallelism());
        
        // 3. 创建 Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.getProperty("kafka.bootstrap.servers"))
            .setTopics(config.getProperty("kafka.source.topic"))
            .setGroupId(config.getProperty("kafka.group.id", "data-quality-monitor"))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> sourceStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),  // 先不分配 watermark
            "Kafka Source"
        );
        
        // 4. 解析订单数据并分配 Watermark (使用 Order.eventTime)
        DataStream<Order> orderStream = sourceStream
            .map(new MetricsMapFunction<String>("source"))
            .map(json -> {
                try {
                    Order order = Order.fromJson(json);
                    if (order == null) {
                        LOG.warn("Failed to parse order: {}", json);
                        return null;
                    }
                    if (order.getOrderId() == null) {
                        order.setOrderId("UNKNOWN_" + System.currentTimeMillis());
                        LOG.warn("Order with null orderId, assigned default: {}", order.getOrderId());
                    }
                    // 确保 eventTime 不为空
                    if (order.getEventTime() == null) {
                        order.setEventTime(System.currentTimeMillis());
                        LOG.warn("Order {} has null eventTime, using current time", order.getOrderId());
                    }
                    return order;
                } catch (Exception e) {
                    LOG.error("Failed to parse order: {}", json, e);
                    return null;
                }
            })
            .filter(order -> order != null)
            .name("Parse Order")
            // 在解析后分配 Watermark,使用 Order.eventTime
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((order, timestamp) -> order.getEventTime())
            );
        
        // 5. 数据去重（使用 RocksDB 存储 10 亿订单 ID，约 60 GB 状态）
        DataStream<Order> dedupedStream = orderStream
            .keyBy(Order::getOrderId)  // 按订单 ID 分组
            .process(new DeduplicationProcessFunction())
            .name("Deduplication (RocksDB 60GB State)");
        
        LOG.info("Deduplication enabled: 24h window, ~60 GB state for 1 billion orders");
        
        // ========== RocksDB 性能监控（简化版） ==========
        // 监控数据流量、吞吐量、延迟
        // 每 60 秒输出性能报告
        DataStream<Order> monitoredStream = dedupedStream
            .map(new SimpleRocksDBMonitor<>())
            .name("RocksDB Performance Monitor")
            .uid("rocksdb-performance-monitor");
        
        LOG.info("RocksDB Performance Monitoring enabled (60s interval)");
        
        // 6. 规则引擎校验
        SingleOutputStreamOperator<Order> validatedStream = monitoredStream  // 使用监控后的流
            .keyBy(Order::getOrderId)  // 按订单 ID 分组
            .process(new RuleEngineProcessFunction(
                config.getProperty("mysql.url"),
                config.getProperty("mysql.username"),
                config.getProperty("mysql.password"),
                config.getProperty("redis.host"),
                Integer.parseInt(config.getProperty("redis.port", "6379"))
            ))
            .name("Rule Engine Validation");
        
        // 7. 正常数据 → Kafka (添加 Sink Metrics)
        DataStream<Order> validOrders = validatedStream;
        KafkaSink<String> validKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getProperty("kafka.bootstrap.servers"))
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(config.getProperty("kafka.normal.topic"))
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();
        
        validOrders
            .map(new MetricsMapFunction<Order>("valid_sink"))
            .map(Order::toJson)
            .sinkTo(validKafkaSink)
            .name("Valid Orders to Kafka");
        
        // 8. 异常数据 → Kafka (添加 Sink Metrics)
        DataStream<Order> invalidOrders = validatedStream
            .getSideOutput(RuleEngineProcessFunction.INVALID_DATA_TAG);
        
        KafkaSink<String> invalidKafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getProperty("kafka.bootstrap.servers"))
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(config.getProperty("kafka.invalid.topic"))
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();
        
        invalidOrders
            .map(new MetricsMapFunction<Order>("invalid_sink"))
            .map(Order::toJson)
            .sinkTo(invalidKafkaSink)
            .name("Invalid Orders to Kafka");
        
        // 9. 质量指标聚合 (1分钟窗口)
        DataStream<ValidationResult> metricsStream = validatedStream
            .getSideOutput(RuleEngineProcessFunction.METRICS_TAG)
            // Side Output 需要重新分配 Watermark（继承 Order 的 eventTime）
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<ValidationResult>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((result, timestamp) -> result.getOrder().getEventTime())
            );
        
        DataStream<QualityMetrics> aggregatedMetrics = metricsStream
            .keyBy(result -> "ALL") // 全局聚合
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new QualityMetricsAggregator())
            .name("Aggregate Quality Metrics");
        
        // 10. 质量指标 → ClickHouse
        aggregatedMetrics.addSink(new ClickHouseSink(
            config.getProperty("clickhouse.url"),
            config.getProperty("clickhouse.username", "default"),
            config.getProperty("clickhouse.password", "")
        )).name("Quality Metrics to ClickHouse");
        
        // 11. 规则监控指标 → ClickHouse (带查询性能监控)
        DataStream<com.dataplatform.quality.model.RuleExecutionMetric> ruleMetrics = validatedStream
            .getSideOutput(RuleEngineProcessFunction.RULE_METRICS_TAG);
        
        ruleMetrics.addSink(new com.dataplatform.quality.sink.ClickHouseQueryMetricsSink(
            config.getProperty("clickhouse.url"),
            config.getProperty("clickhouse.username", "default"),
            config.getProperty("clickhouse.password", "")
        )).name("Rule Metrics to ClickHouse (with Query Monitoring)");
        
        // 12. 执行任务
        LOG.info("Starting Data Quality Monitor Job...");
        env.execute("Data Quality Monitor Job");
    }
    
    /**
     * 加载配置文件
     */
    private static Properties loadConfig() {
        Properties config = new Properties();
        
        try (InputStream input = DataQualityMonitorJob.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            
            if (input == null) {
                LOG.warn("Unable to find application.properties, using default config");
                return getDefaultConfig();
            }
            
            config.load(input);
            LOG.info("Configuration loaded from application.properties");
            
        } catch (Exception e) {
            LOG.error("Error loading configuration", e);
            return getDefaultConfig();
        }
        
        return config;
    }
    
    /**
     * 默认配置
     */
    private static Properties getDefaultConfig() {
        Properties config = new Properties();
        
        // Kafka
        config.setProperty("kafka.bootstrap.servers", "192.168.128.141:9092");
        config.setProperty("kafka.source.topic", "orders");
        config.setProperty("kafka.normal.topic", "orders_valid");
        config.setProperty("kafka.invalid.topic", "orders_invalid");
        config.setProperty("kafka.group.id", "data-quality-monitor");
        
        // MySQL
        config.setProperty("mysql.url", "jdbc:mysql://192.168.128.141:3306/data_quality?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setProperty("mysql.username", "root");
        config.setProperty("mysql.password", "123456");
        
        // Redis
        config.setProperty("redis.host", "192.168.128.141");
        config.setProperty("redis.port", "6379");
        
        // ClickHouse
        config.setProperty("clickhouse.url", "jdbc:clickhouse://192.168.128.141:8123/default");
        config.setProperty("clickhouse.username", "default");
        config.setProperty("clickhouse.password", "");
        
        // Flink
        config.setProperty("flink.parallelism", "4");
        
        return config;
    }
}
