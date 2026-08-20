package com.dataplatform.quality.config;

import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.metrics.MetricGroup;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

/**
 * 高级 RocksDB 配置 - 带性能监控和自适应调优
 * 
 * 源码知识点：
 * 1. RocksDB LSM Tree 结构：MemTable → Immutable MemTable → SST Files (L0-L6)
 * 2. Compaction 策略：Level Compaction vs Universal Compaction
 * 3. Block Cache：缓存 SST 文件的数据块，减少磁盘 I/O
 * 4. Write Buffer：内存中的写缓冲区，批量刷盘提高性能
 * 5. Bloom Filter：快速判断 key 是否存在，减少读放大
 * 
 * 实战目标：
 * 1. 监控 RocksDB 内部指标（Compaction、Cache、I/O）
 * 2. 分析性能瓶颈（写放大、读放大、空间放大）
 * 3. 提供调优建议
 */
public class AdvancedRocksDBConfig implements RocksDBOptionsFactory {
    
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedRocksDBConfig.class);
    
    /** RocksDB 统计信息（用于监控） */
    private Statistics statistics;
    
    /** 配置模式 */
    private final ConfigMode mode;
    
    /** 是否启用详细日志 */
    private final boolean enableVerboseLogging;
    
    public enum ConfigMode {
        /** 高吞吐：优化写入性能，适合数据生成器场景 */
        HIGH_THROUGHPUT,
        
        /** 低延迟：优化读取性能，适合实时查询场景 */
        LOW_LATENCY,
        
        /** 大状态：优化内存和磁盘使用，适合亿级状态场景 */
        LARGE_STATE
    }
    
    public AdvancedRocksDBConfig(ConfigMode mode) {
        this(mode, false);
    }
    
    public AdvancedRocksDBConfig(ConfigMode mode, boolean enableVerboseLogging) {
        this.mode = mode;
        this.enableVerboseLogging = enableVerboseLogging;
        LOG.info("AdvancedRocksDBConfig initialized: mode={}, verboseLogging={}", 
            mode, enableVerboseLogging);
    }
    
    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, 
                                     Collection<AutoCloseable> handlesToClose) {
        
        LOG.info("=== Creating DBOptions with mode: {} ===", mode);
        
        // 创建统计对象（用于监控）
        statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        handlesToClose.add(statistics);
        
        currentOptions
            // 1. 后台线程配置
            .setMaxBackgroundJobs(getMaxBackgroundJobs())
            .setMaxBackgroundCompactions(getMaxBackgroundCompactions())
            .setMaxBackgroundFlushes(getMaxBackgroundFlushes())
            
            // 2. 并行压实（利用多核）
            .setMaxSubcompactions(4)
            
            // 3. 文件句柄限制（-1 表示无限制）
            .setMaxOpenFiles(-1)
            
            // 4. 启用统计（关键：用于性能监控）
            .setStatistics(statistics)
            .setStatsDumpPeriodSec(60)  // 每 60 秒输出统计信息
            
            // 5. WAL 配置
            .setMaxTotalWalSize(getMaxTotalWalSize())
            .setWalSizeLimitMB(0)  // 不限制 WAL 大小
            .setWalTtlSeconds(0)   // WAL 不过期
            
            // 6. 日志配置
            .setInfoLogLevel(enableVerboseLogging ? InfoLogLevel.INFO_LEVEL : InfoLogLevel.WARN_LEVEL)
            .setKeepLogFileNum(3)
            .setMaxLogFileSize(100 * 1024 * 1024);  // 100MB
        
        // 打印配置信息
        logDBOptions(currentOptions);
        
        return currentOptions;
    }
    
    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions,
            Collection<AutoCloseable> handlesToClose) {
        
        LOG.info("=== Creating ColumnFamilyOptions with mode: {} ===", mode);
        
        // 根据模式选择配置
        ColumnFamilyOptions options;
        switch (mode) {
            case HIGH_THROUGHPUT:
                options = configureHighThroughput(currentOptions, handlesToClose);
                break;
            case LOW_LATENCY:
                options = configureLowLatency(currentOptions, handlesToClose);
                break;
            case LARGE_STATE:
                options = configureLargeState(currentOptions, handlesToClose);
                break;
            default:
                options = currentOptions;
                break;
        }
        
        // 打印配置信息
        logColumnFamilyOptions(options);
        
        return options;
    }
    
    /**
     * 高吞吐配置（写入密集）
     * 
     * 源码知识点：
     * - WriteBuffer 越大，刷盘次数越少，写入性能越好
     * - Level 0 文件数量控制：太多会触发 Write Stall
     * - Compaction 触发时机：Level 0 文件数达到阈值
     */
    private ColumnFamilyOptions configureHighThroughput(
            ColumnFamilyOptions options,
            Collection<AutoCloseable> handlesToClose) {
        
        // 1. 大 WriteBuffer（减少刷盘频率）
        // 源码：MemTable 写满后变成 Immutable MemTable，然后刷盘成 SST 文件
        options.setWriteBufferSize(256 * 1024 * 1024);  // 256MB
        options.setMaxWriteBufferNumber(4);  // 最多 4 个 WriteBuffer
        options.setMinWriteBufferNumberToMerge(2);  // 至少 2 个 WriteBuffer 合并后刷盘
        
        // 2. Level 0 参数（控制 Compaction 触发时机）
        // 源码：Level 0 文件数量达到 trigger 值时触发 Compaction
        options.setLevel0FileNumCompactionTrigger(10);  // 10 个文件触发 Compaction
        options.setLevel0SlowdownWritesTrigger(40);     // 40 个文件开始限速
        options.setLevel0StopWritesTrigger(60);         // 60 个文件停止写入
        
        // 3. 文件大小配置（减少文件数量）
        options.setTargetFileSizeBase(256 * 1024 * 1024);  // Level 1 文件大小 256MB
        options.setMaxBytesForLevelBase(1024 * 1024 * 1024);  // Level 1 总大小 1GB
        options.setTargetFileSizeMultiplier(2);  // 每层文件大小翻倍
        options.setMaxBytesForLevelMultiplier(10);  // 每层总大小 10 倍
        
        // 4. Block Cache 配置（提高读性能）
        BlockBasedTableConfig tableConfig = createBlockBasedTableConfig(
            2L * 1024 * 1024 * 1024,  // 2GB Block Cache
            16 * 1024,  // 16KB Block Size
            handlesToClose
        );
        options.setTableFormatConfig(tableConfig);
        
        // 5. Compaction 策略
        options.setCompactionStyle(CompactionStyle.LEVEL);
        
        // 6. 压缩配置
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);  // 快速压缩
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);  // 最底层强压缩
        
        LOG.info("High throughput config applied: writeBuffer=256MB, blockCache=2GB");
        
        return options;
    }
    
    /**
     * 低延迟配置（读取密集）
     * 
     * 源码知识点：
     * - Block Cache 越大，缓存命中率越高，读取越快
     * - Bloom Filter 可以快速判断 key 是否存在，减少磁盘 I/O
     * - Level 0 文件越少，读放大越小
     */
    private ColumnFamilyOptions configureLowLatency(
            ColumnFamilyOptions options,
            Collection<AutoCloseable> handlesToClose) {
        
        // 1. 大 Block Cache（提高缓存命中率）
        // 源码：Block Cache 缓存 SST 文件的数据块，命中后直接返回，无需读磁盘
        BlockBasedTableConfig tableConfig = createBlockBasedTableConfig(
            4L * 1024 * 1024 * 1024,  // 4GB Block Cache
            8 * 1024,  // 8KB Block Size（更小的 Block，更精确的缓存）
            handlesToClose
        );
        
        // 2. 启用 Bloom Filter（减少读放大）
        // 源码：Bloom Filter 可以快速判断 key 是否在 SST 文件中，避免无效读取
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        tableConfig.setWholeKeyFiltering(true);
        
        // 3. 缓存索引和过滤器（减少磁盘 I/O）
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        
        options.setTableFormatConfig(tableConfig);
        
        // 4. 减少 Level 数量（减少读放大）
        // 源码：读取时需要查找多个 Level，Level 越少，读放大越小
        options.setNumLevels(4);
        
        // 5. 更激进的 Compaction（保持 Level 0 文件少）
        options.setLevel0FileNumCompactionTrigger(4);
        options.setLevel0SlowdownWritesTrigger(20);
        options.setLevel0StopWritesTrigger(36);
        
        // 6. 适中的 WriteBuffer
        options.setWriteBufferSize(128 * 1024 * 1024);  // 128MB
        options.setMaxWriteBufferNumber(3);
        
        // 7. Compaction 策略
        options.setCompactionStyle(CompactionStyle.LEVEL);
        
        LOG.info("Low latency config applied: blockCache=4GB, numLevels=4");
        
        return options;
    }
    
    /**
     * 大状态配置（亿级状态）
     * 
     * 源码知识点：
     * - Universal Compaction 更适合大状态：减少写放大，但增加读放大
     * - 分区索引：将索引分成多个部分，减少内存占用
     * - 强压缩：减少磁盘占用，但增加 CPU 开销
     */
    private ColumnFamilyOptions configureLargeState(
            ColumnFamilyOptions options,
            Collection<AutoCloseable> handlesToClose) {
        
        // 1. 大 WriteBuffer（减少刷盘次数）
        options.setWriteBufferSize(512 * 1024 * 1024);  // 512MB
        options.setMaxWriteBufferNumber(6);
        options.setMinWriteBufferNumberToMerge(2);
        
        // 2. 大文件大小（减少文件数量）
        options.setTargetFileSizeBase(512 * 1024 * 1024);  // 512MB
        options.setMaxBytesForLevelBase(2L * 1024 * 1024 * 1024);  // 2GB
        
        // 3. Universal Compaction（更适合大状态）
        // 源码：Universal Compaction 将相邻的 SST 文件合并，减少写放大
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        
        // 4. Block Cache 配置
        BlockBasedTableConfig tableConfig = createBlockBasedTableConfig(
            3L * 1024 * 1024 * 1024,  // 3GB Block Cache
            16 * 1024,  // 16KB Block Size
            handlesToClose
        );
        
        // 5. 分区索引（减少内存占用）
        // 源码：将索引分成多个部分，只加载需要的部分到内存
        tableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
        tableConfig.setPartitionFilters(true);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        
        // 6. Bloom Filter
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        
        options.setTableFormatConfig(tableConfig);
        
        // 7. 强压缩（减少磁盘占用）
        options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        
        // 8. Level 0 参数
        options.setLevel0FileNumCompactionTrigger(8);
        options.setLevel0SlowdownWritesTrigger(30);
        options.setLevel0StopWritesTrigger(50);
        
        LOG.info("Large state config applied: writeBuffer=512MB, compaction=UNIVERSAL");
        
        return options;
    }
    
    /**
     * 创建 BlockBasedTableConfig
     */
    private BlockBasedTableConfig createBlockBasedTableConfig(
            long blockCacheSize,
            int blockSize,
            Collection<AutoCloseable> handlesToClose) {
        
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        
        // 1. Block Cache
        Cache blockCache = new LRUCache(blockCacheSize);
        handlesToClose.add(blockCache);
        tableConfig.setBlockCache(blockCache);
        
        // 2. Block Size
        tableConfig.setBlockSize(blockSize);
        
        // 3. 缓存索引和过滤器
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        
        // 4. Bloom Filter
        BloomFilter bloomFilter = new BloomFilter(10, false);
        handlesToClose.add(bloomFilter);
        tableConfig.setFilterPolicy(bloomFilter);
        
        return tableConfig;
    }
    
    /**
     * 获取最大后台任务数
     */
    private int getMaxBackgroundJobs() {
        switch (mode) {
            case HIGH_THROUGHPUT:
                return 16;  // 高吞吐需要更多后台线程
            case LOW_LATENCY:
                return 8;
            case LARGE_STATE:
                return 16;
            default:
                return 8;
        }
    }
    
    /**
     * 获取最大后台 Compaction 数
     */
    private int getMaxBackgroundCompactions() {
        switch (mode) {
            case HIGH_THROUGHPUT:
                return 8;
            case LOW_LATENCY:
                return 4;
            case LARGE_STATE:
                return 8;
            default:
                return 4;
        }
    }
    
    /**
     * 获取最大后台 Flush 数
     */
    private int getMaxBackgroundFlushes() {
        switch (mode) {
            case HIGH_THROUGHPUT:
                return 4;
            case LOW_LATENCY:
                return 2;
            case LARGE_STATE:
                return 4;
            default:
                return 2;
        }
    }
    
    /**
     * 获取最大 WAL 大小
     */
    private long getMaxTotalWalSize() {
        switch (mode) {
            case HIGH_THROUGHPUT:
                return 512 * 1024 * 1024L;  // 512MB
            case LOW_LATENCY:
                return 256 * 1024 * 1024L;  // 256MB
            case LARGE_STATE:
                return 1024 * 1024 * 1024L; // 1GB
            default:
                return 512 * 1024 * 1024L;
        }
    }
    
    /**
     * 打印 DBOptions 配置
     */
    private void logDBOptions(DBOptions options) {
        LOG.info("DBOptions configuration:");
        LOG.info("  - maxBackgroundJobs: {}", options.maxBackgroundJobs());
        LOG.info("  - maxBackgroundCompactions: {}", options.maxBackgroundCompactions());
        LOG.info("  - maxBackgroundFlushes: {}", options.maxBackgroundFlushes());
        LOG.info("  - maxSubcompactions: {}", options.maxSubcompactions());
        LOG.info("  - maxOpenFiles: {}", options.maxOpenFiles());
        LOG.info("  - maxTotalWalSize: {} MB", options.maxTotalWalSize() / 1024 / 1024);
    }
    
    /**
     * 打印 ColumnFamilyOptions 配置
     */
    private void logColumnFamilyOptions(ColumnFamilyOptions options) {
        LOG.info("ColumnFamilyOptions configuration:");
        LOG.info("  - writeBufferSize: {} MB", options.writeBufferSize() / 1024 / 1024);
        LOG.info("  - maxWriteBufferNumber: {}", options.maxWriteBufferNumber());
        LOG.info("  - level0FileNumCompactionTrigger: {}", options.level0FileNumCompactionTrigger());
        LOG.info("  - level0SlowdownWritesTrigger: {}", options.level0SlowdownWritesTrigger());
        LOG.info("  - level0StopWritesTrigger: {}", options.level0StopWritesTrigger());
        LOG.info("  - targetFileSizeBase: {} MB", options.targetFileSizeBase() / 1024 / 1024);
        LOG.info("  - compactionStyle: {}", options.compactionStyle());
        LOG.info("  - compressionType: {}", options.compressionType());
    }
    
    /**
     * 获取 RocksDB 统计信息
     */
    public Statistics getStatistics() {
        return statistics;
    }
}
