package com.dataplatform.quality.config;

import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * 优化的RocksDB配置工厂
 * 
 * 适用场景：
 * - 高吞吐写入（UV计算、实时聚合）
 * - 大状态场景（亿级状态）
 * - 低延迟读取（实时查询）
 * 
 * 核心优化：
 * 1. 增大WriteBuffer，减少刷盘频率
 * 2. 增加后台线程，加快Compaction
 * 3. 增大Block Cache，提高读性能
 * 4. 调整Level 0参数，避免Write Stall
 */
public class OptimizedRocksDBConfig implements RocksDBOptionsFactory {
    
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedRocksDBConfig.class);
    
    /** 配置模式 */
    public enum ConfigMode {
        HIGH_THROUGHPUT,  // 高吞吐（写入密集）
        LOW_LATENCY,      // 低延迟（读取密集）
        LARGE_STATE       // 大状态（亿级状态）
    }
    
    private final ConfigMode mode;
    
    public OptimizedRocksDBConfig(ConfigMode mode) {
        this.mode = mode;
        LOG.info("RocksDB config mode: {}", mode);
    }
    
    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, 
                                     Collection<AutoCloseable> handlesToClose) {
        
        LOG.info("Creating DBOptions with mode: {}", mode);
        
        // 基础配置（所有模式通用）
        currentOptions
            // 增加后台线程
            .setMaxBackgroundJobs(8)
            .setMaxBackgroundCompactions(4)
            .setMaxBackgroundFlushes(2)
            
            // 启用并行压实
            .setMaxSubcompactions(4)
            
            // 增加打开文件数限制
            .setMaxOpenFiles(-1)
            
            // 启用统计（用于监控）
            .setStatistics(new Statistics())
            .setStatsDumpPeriodSec(60)
            
            // WAL配置
            .setMaxTotalWalSize(512 * 1024 * 1024);  // 512MB
        
        // 根据模式调整
        switch (mode) {
            case HIGH_THROUGHPUT:
                // 高吞吐模式：优先写入性能
                currentOptions.setMaxBackgroundJobs(16);
                currentOptions.setMaxBackgroundCompactions(8);
                break;
                
            case LOW_LATENCY:
                // 低延迟模式：优先读取性能
                currentOptions.setMaxBackgroundJobs(8);
                break;
                
            case LARGE_STATE:
                // 大状态模式：平衡性能和资源
                currentOptions.setMaxBackgroundJobs(16);
                currentOptions.setMaxTotalWalSize(1024 * 1024 * 1024);  // 1GB
                break;
        }
        
        LOG.info("DBOptions configured: maxBackgroundJobs={}, maxBackgroundCompactions={}", 
            currentOptions.maxBackgroundJobs(), 
            currentOptions.maxBackgroundCompactions());
        
        return currentOptions;
    }
    
    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions,
            Collection<AutoCloseable> handlesToClose) {
        
        LOG.info("Creating ColumnFamilyOptions with mode: {}", mode);
        
        // 根据模式选择配置
        switch (mode) {
            case HIGH_THROUGHPUT:
                return configureHighThroughput(currentOptions);
            case LOW_LATENCY:
                return configureLowLatency(currentOptions);
            case LARGE_STATE:
                return configureLargeState(currentOptions);
            default:
                return currentOptions;
        }
    }
    
    /**
     * 高吞吐配置（写入密集）
     */
    private ColumnFamilyOptions configureHighThroughput(ColumnFamilyOptions options) {
        
        // 1. 增大WriteBuffer（减少刷盘频率）
        options.setWriteBufferSize(256 * 1024 * 1024);  // 256MB
        options.setMaxWriteBufferNumber(4);
        options.setMinWriteBufferNumberToMerge(2);
        
        // 2. 调整Level 0参数（延迟Compaction触发）
        options.setLevel0FileNumCompactionTrigger(10);
        options.setLevel0SlowdownWritesTrigger(40);
        options.setLevel0StopWritesTrigger(60);
        
        // 3. 增大文件大小（减少文件数量）
        options.setTargetFileSizeBase(256 * 1024 * 1024);  // 256MB
        options.setMaxBytesForLevelBase(1024 * 1024 * 1024);  // 1GB
        options.setTargetFileSizeMultiplier(2);
        options.setMaxBytesForLevelMultiplier(10);
        
        // 4. 配置Block Cache（2GB）
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(2L * 1024 * 1024 * 1024);  // 2GB
        tableConfig.setBlockSize(16 * 1024);  // 16KB
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        
        // 5. 启用Bloom Filter
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        
        options.setTableFormatConfig(tableConfig);
        
        // 6. 使用Level Compaction
        options.setCompactionStyle(CompactionStyle.LEVEL);
        
        // 7. 启用压缩
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        
        LOG.info("High throughput config: writeBuffer=256MB, blockCache=2GB, level0Trigger=10");
        
        return options;
    }
    
    /**
     * 低延迟配置（读取密集）
     */
    private ColumnFamilyOptions configureLowLatency(ColumnFamilyOptions options) {
        
        // 1. 优先优化读性能
        
        // 2. 增大Block Cache（4GB）
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(4L * 1024 * 1024 * 1024);  // 4GB
        tableConfig.setBlockSize(8 * 1024);  // 8KB（更小的Block，更精确的缓存）
        
        // 3. 启用Bloom Filter（减少读放大）
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        tableConfig.setWholeKeyFiltering(true);
        
        // 4. 缓存索引和过滤器
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        
        options.setTableFormatConfig(tableConfig);
        
        // 5. 减少Level数量（减少读放大）
        options.setNumLevels(4);
        
        // 6. 更激进的Compaction（保持Level 0文件少）
        options.setLevel0FileNumCompactionTrigger(4);
        options.setLevel0SlowdownWritesTrigger(20);
        options.setLevel0StopWritesTrigger(36);
        
        // 7. 适中的WriteBuffer
        options.setWriteBufferSize(128 * 1024 * 1024);  // 128MB
        options.setMaxWriteBufferNumber(3);
        
        // 8. 使用Level Compaction
        options.setCompactionStyle(CompactionStyle.LEVEL);
        
        LOG.info("Low latency config: blockCache=4GB, numLevels=4, level0Trigger=4");
        
        return options;
    }
    
    /**
     * 大状态配置（亿级状态）
     */
    private ColumnFamilyOptions configureLargeState(ColumnFamilyOptions options) {
        
        // 1. 大WriteBuffer（减少刷盘次数）
        options.setWriteBufferSize(512 * 1024 * 1024);  // 512MB
        options.setMaxWriteBufferNumber(6);
        options.setMinWriteBufferNumberToMerge(2);
        
        // 2. 大文件大小（减少文件数量）
        options.setTargetFileSizeBase(512 * 1024 * 1024);  // 512MB
        options.setMaxBytesForLevelBase(2L * 1024 * 1024 * 1024);  // 2GB
        options.setTargetFileSizeMultiplier(2);
        options.setMaxBytesForLevelMultiplier(10);
        
        // 3. 使用Universal Compaction（更适合大状态）
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        
        // 4. 配置Block Cache（3GB）
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(3L * 1024 * 1024 * 1024);  // 3GB
        tableConfig.setBlockSize(16 * 1024);  // 16KB
        
        // 5. 启用分区索引（减少内存占用）
        tableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
        tableConfig.setPartitionFilters(true);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        
        // 6. 启用Bloom Filter
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        
        options.setTableFormatConfig(tableConfig);
        
        // 7. 使用强压缩（减少磁盘占用）
        options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        
        // 8. 调整Level 0参数
        options.setLevel0FileNumCompactionTrigger(8);
        options.setLevel0SlowdownWritesTrigger(30);
        options.setLevel0StopWritesTrigger(50);
        
        LOG.info("Large state config: writeBuffer=512MB, fileSize=512MB, compaction=UNIVERSAL");
        
        return options;
    }
}
