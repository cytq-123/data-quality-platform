package com.dataplatform.quality.monitor;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB 性能监控器
 * 
 * 功能：
 * 1. 监控 RocksDB 内部指标（Compaction、Cache、I/O）
 * 2. 分析性能瓶颈（写放大、读放大、空间放大）
 * 3. 提供调优建议
 * 
 * 源码知识点：
 * 1. RocksDB Statistics：记录各种操作的计数器
 * 2. TickerType：RocksDB 内部指标类型
 * 3. Flink Metrics：将 RocksDB 指标暴露给 Prometheus
 * 
 * 关键指标：
 * - BLOCK_CACHE_HIT：Block Cache 命中次数
 * - BLOCK_CACHE_MISS：Block Cache 未命中次数
 * - BYTES_WRITTEN：写入字节数
 * - BYTES_READ：读取字节数
 * - COMPACTION_KEY_DROP_OBSOLETE：Compaction 删除的过期 key 数量
 * - NUMBER_KEYS_WRITTEN：写入 key 数量
 * - NUMBER_KEYS_READ：读取 key 数量
 */
public class RocksDBPerformanceMonitor<T> extends RichMapFunction<T, T> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBPerformanceMonitor.class);
    
    /** RocksDB 统计信息 */
    private final Statistics statistics;
    
    /** 上一次统计时间 */
    private long lastReportTime;
    
    /** 上一次统计的指标值 */
    private long lastBytesWritten;
    private long lastBytesRead;
    private long lastKeysWritten;
    private long lastKeysRead;
    private long lastBlockCacheHit;
    private long lastBlockCacheMiss;
    
    /** 统计间隔（毫秒） */
    private static final long REPORT_INTERVAL_MS = 60_000;  // 60 秒
    
    public RocksDBPerformanceMonitor(Statistics statistics) {
        this.statistics = statistics;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        lastReportTime = System.currentTimeMillis();
        
        // 注册 Flink Metrics
        registerMetrics();
        
        LOG.info("RocksDBPerformanceMonitor initialized");
    }
    
    @Override
    public T map(T value) throws Exception {
        // 检查 statistics 是否为 null
        if (statistics == null) {
            // Statistics 未初始化，跳过监控
            return value;
        }
        
        // 定期输出性能报告
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastReportTime >= REPORT_INTERVAL_MS) {
            reportPerformance();
            lastReportTime = currentTime;
        }
        
        return value;
    }
    
    /**
     * 注册 Flink Metrics
     * 
     * 这些指标会被 Prometheus 抓取，可以在 Grafana 中可视化
     */
    private void registerMetrics() {
        // 检查 statistics 是否为 null
        if (statistics == null) {
            LOG.warn("RocksDB Statistics is null, metrics will not be registered");
            return;
        }
        
        MetricGroup rocksdbGroup = getRuntimeContext()
            .getMetricGroup()
            .addGroup("rocksdb");
        
        // 1. Block Cache 指标
        rocksdbGroup.gauge("block_cache_hit_rate", new Gauge<Double>() {
            @Override
            public Double getValue() {
                return getBlockCacheHitRate();
            }
        });
        
        rocksdbGroup.gauge("block_cache_hit", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT);
            }
        });
        
        rocksdbGroup.gauge("block_cache_miss", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS);
            }
        });
        
        // 2. I/O 指标
        rocksdbGroup.gauge("bytes_written", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.BYTES_WRITTEN);
            }
        });
        
        rocksdbGroup.gauge("bytes_read", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.BYTES_READ);
            }
        });
        
        // 3. Compaction 指标
        rocksdbGroup.gauge("compaction_key_drop_obsolete", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_OBSOLETE);
            }
        });
        
        rocksdbGroup.gauge("compaction_key_drop_newer_entry", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY);
            }
        });
        
        // 4. 写入指标
        rocksdbGroup.gauge("number_keys_written", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
            }
        });
        
        rocksdbGroup.gauge("number_keys_updated", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.NUMBER_KEYS_UPDATED);
            }
        });
        
        // 5. 读取指标
        rocksdbGroup.gauge("number_keys_read", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.NUMBER_KEYS_READ);
            }
        });
        
        // 6. Stall 指标（写入阻塞）
        rocksdbGroup.gauge("stall_micros", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.STALL_MICROS);
            }
        });
        
        // 7. WAL 指标
        rocksdbGroup.gauge("wal_file_synced", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.WAL_FILE_SYNCED);
            }
        });
        
        rocksdbGroup.gauge("wal_file_bytes", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return statistics.getTickerCount(TickerType.WAL_FILE_BYTES);
            }
        });
        
        LOG.info("RocksDB metrics registered");
    }
    
    /**
     * 输出性能报告
     */
    private void reportPerformance() {
        // 再次检查 statistics
        if (statistics == null) {
            LOG.warn("RocksDB Statistics is null, skipping performance report");
            return;
        }
        
        LOG.info("========== RocksDB Performance Report ==========");
        
        // 1. Block Cache 性能
        reportBlockCachePerformance();
        
        // 2. I/O 性能
        reportIOPerformance();
        
        // 3. Compaction 性能
        reportCompactionPerformance();
        
        // 4. 写入性能
        reportWritePerformance();
        
        // 5. 读取性能
        reportReadPerformance();
        
        // 6. Stall 情况
        reportStallInfo();
        
        // 7. 性能分析和调优建议
        analyzePerformance();
        
        LOG.info("===============================================");
    }
    
    /**
     * Block Cache 性能报告
     */
    private void reportBlockCachePerformance() {
        long blockCacheHit = statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT);
        long blockCacheMiss = statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS);
        
        long hitDelta = blockCacheHit - lastBlockCacheHit;
        long missDelta = blockCacheMiss - lastBlockCacheMiss;
        
        double hitRate = getBlockCacheHitRate();
        
        LOG.info("Block Cache:");
        LOG.info("  - Hit: {} (+{})", blockCacheHit, hitDelta);
        LOG.info("  - Miss: {} (+{})", blockCacheMiss, missDelta);
        LOG.info("  - Hit Rate: {:.2f}%", hitRate * 100);
        
        lastBlockCacheHit = blockCacheHit;
        lastBlockCacheMiss = blockCacheMiss;
    }
    
    /**
     * I/O 性能报告
     */
    private void reportIOPerformance() {
        long bytesWritten = statistics.getTickerCount(TickerType.BYTES_WRITTEN);
        long bytesRead = statistics.getTickerCount(TickerType.BYTES_READ);
        
        long writtenDelta = bytesWritten - lastBytesWritten;
        long readDelta = bytesRead - lastBytesRead;
        
        LOG.info("I/O:");
        LOG.info("  - Bytes Written: {} MB (+{} MB)", 
            bytesWritten / 1024 / 1024, writtenDelta / 1024 / 1024);
        LOG.info("  - Bytes Read: {} MB (+{} MB)", 
            bytesRead / 1024 / 1024, readDelta / 1024 / 1024);
        LOG.info("  - Write Throughput: {:.2f} MB/s", 
            writtenDelta / 1024.0 / 1024.0 / (REPORT_INTERVAL_MS / 1000.0));
        LOG.info("  - Read Throughput: {:.2f} MB/s", 
            readDelta / 1024.0 / 1024.0 / (REPORT_INTERVAL_MS / 1000.0));
        
        lastBytesWritten = bytesWritten;
        lastBytesRead = bytesRead;
    }
    
    /**
     * Compaction 性能报告
     */
    private void reportCompactionPerformance() {
        long keyDropObsolete = statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_OBSOLETE);
        long keyDropNewerEntry = statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY);
        
        LOG.info("Compaction:");
        LOG.info("  - Keys Dropped (Obsolete): {}", keyDropObsolete);
        LOG.info("  - Keys Dropped (Newer Entry): {}", keyDropNewerEntry);
    }
    
    /**
     * 写入性能报告
     */
    private void reportWritePerformance() {
        long keysWritten = statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
        long keysUpdated = statistics.getTickerCount(TickerType.NUMBER_KEYS_UPDATED);
        
        long writtenDelta = keysWritten - lastKeysWritten;
        
        LOG.info("Write:");
        LOG.info("  - Keys Written: {} (+{})", keysWritten, writtenDelta);
        LOG.info("  - Keys Updated: {}", keysUpdated);
        LOG.info("  - Write QPS: {:.2f}", 
            writtenDelta / (REPORT_INTERVAL_MS / 1000.0));
        
        lastKeysWritten = keysWritten;
    }
    
    /**
     * 读取性能报告
     */
    private void reportReadPerformance() {
        long keysRead = statistics.getTickerCount(TickerType.NUMBER_KEYS_READ);
        
        long readDelta = keysRead - lastKeysRead;
        
        LOG.info("Read:");
        LOG.info("  - Keys Read: {} (+{})", keysRead, readDelta);
        LOG.info("  - Read QPS: {:.2f}", 
            readDelta / (REPORT_INTERVAL_MS / 1000.0));
        
        lastKeysRead = keysRead;
    }
    
    /**
     * Stall 信息报告
     */
    private void reportStallInfo() {
        long stallMicros = statistics.getTickerCount(TickerType.STALL_MICROS);
        
        if (stallMicros > 0) {
            LOG.warn("Write Stall Detected:");
            LOG.warn("  - Stall Time: {} ms", stallMicros / 1000);
            LOG.warn("  - This indicates RocksDB is under pressure!");
        }
    }
    
    /**
     * 性能分析和调优建议
     */
    private void analyzePerformance() {
        LOG.info("Performance Analysis:");
        
        // 1. Block Cache 命中率分析
        double hitRate = getBlockCacheHitRate();
        if (hitRate < 0.8) {
            LOG.warn("  ⚠️  Block Cache hit rate is low ({:.2f}%)", hitRate * 100);
            LOG.warn("  💡 Suggestion: Increase Block Cache size");
        } else {
            LOG.info("  ✅ Block Cache hit rate is good ({:.2f}%)", hitRate * 100);
        }
        
        // 2. 写放大分析
        long bytesWritten = statistics.getTickerCount(TickerType.BYTES_WRITTEN);
        long keysWritten = statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
        if (keysWritten > 0) {
            long avgValueSize = bytesWritten / keysWritten;
            LOG.info("  - Average value size: {} bytes", avgValueSize);
        }
        
        // 3. Stall 分析
        long stallMicros = statistics.getTickerCount(TickerType.STALL_MICROS);
        if (stallMicros > 0) {
            LOG.warn("  ⚠️  Write stall detected ({} ms)", stallMicros / 1000);
            LOG.warn("  💡 Suggestion: Increase maxBackgroundJobs or level0FileNumCompactionTrigger");
        }
        
        // 4. Compaction 效率分析
        long keyDropObsolete = statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_OBSOLETE);
        if (keyDropObsolete > 0) {
            LOG.info("  ✅ Compaction is cleaning up obsolete keys: {}", keyDropObsolete);
        }
    }
    
    /**
     * 计算 Block Cache 命中率
     */
    private double getBlockCacheHitRate() {
        long hit = statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT);
        long miss = statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS);
        
        if (hit + miss == 0) {
            return 0.0;
        }
        
        return (double) hit / (hit + miss);
    }
}
