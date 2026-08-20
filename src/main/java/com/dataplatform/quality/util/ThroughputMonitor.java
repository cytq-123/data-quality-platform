package com.dataplatform.quality.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 吞吐量监控工具 - 统计 Flink 作业的处理吞吐量
 * 不修改原有业务逻辑，独立统计处理速率
 */
public class ThroughputMonitor {
    
    private static final AtomicLong totalRecords = new AtomicLong(0);
    private static final AtomicLong lastReportTime = new AtomicLong(System.currentTimeMillis());
    private static final AtomicLong lastReportCount = new AtomicLong(0);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static boolean initialized = false;
    
    /**
     * 初始化监控（在 Flink 作业启动时调用一次）
     */
    public static synchronized void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        
        // 每 10 秒报告一次吞吐量
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long currentCount = totalRecords.get();
            long lastCount = lastReportCount.get();
            long lastTime = lastReportTime.get();
            
            long timeDiff = now - lastTime;
            long recordDiff = currentCount - lastCount;
            
            if (timeDiff > 0) {
                // 计算吞吐量：条数/秒
                double throughput = (recordDiff * 1000.0) / timeDiff;
                System.out.println(String.format(
                    "[ThroughputMonitor] Total: %d records, Throughput: %.2f records/sec (%.2f K/sec), Time window: %d ms",
                    currentCount, throughput, throughput / 1000, timeDiff
                ));
            }
            
            lastReportTime.set(now);
            lastReportCount.set(currentCount);
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * 记录处理的一条记录
     * 在 RuleEngineProcessFunction.processElement() 中调用
     */
    public static void recordProcessed() {
        totalRecords.incrementAndGet();
    }
    
    /**
     * 记录处理的多条记录
     */
    public static void recordProcessed(long count) {
        totalRecords.addAndGet(count);
    }
    
    /**
     * 获取总处理记录数
     */
    public static long getTotalRecords() {
        return totalRecords.get();
    }
    
    /**
     * 获取当前吞吐量（条/秒）
     */
    public static double getCurrentThroughput() {
        long now = System.currentTimeMillis();
        long currentCount = totalRecords.get();
        long lastCount = lastReportCount.get();
        long lastTime = lastReportTime.get();
        
        long timeDiff = now - lastTime;
        if (timeDiff <= 0) {
            return 0;
        }
        
        long recordDiff = currentCount - lastCount;
        return (recordDiff * 1000.0) / timeDiff;
    }
    
    /**
     * 打印最终统计
     */
    public static void printFinalStats() {
        long totalCount = totalRecords.get();
        long totalTime = System.currentTimeMillis() - lastReportTime.get();
        
        if (totalTime > 0) {
            double avgThroughput = (totalCount * 1000.0) / totalTime;
            System.out.println("\n========== Throughput Summary ==========");
            System.out.println("Total records processed: " + totalCount);
            System.out.println("Average throughput: " + String.format("%.2f records/sec", avgThroughput));
            System.out.println("Average throughput: " + String.format("%.2f K records/sec", avgThroughput / 1000));
            System.out.println("=========================================\n");
        }
    }
    
    /**
     * 关闭监控
     */
    public static void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
