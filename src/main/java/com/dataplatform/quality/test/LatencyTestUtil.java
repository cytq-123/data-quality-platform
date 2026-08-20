package com.dataplatform.quality.test;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 延迟测试工具类
 * 用于测量端到端延迟：订单生成 → 异常检测
 * 
 * 使用场景：抖音电商恶意刷单检测
 * - 测量从订单生成到检测出异常的延迟
 * - 延迟 = 羊毛党的作案窗口
 * - 延迟越短，拦截越及时，损失越小
 */
public class LatencyTestUtil {
    
    // 延迟记录器（线程安全）
    private static final ConcurrentHashMap<String, Long> orderGenerationTime = new ConcurrentHashMap<>();
    private static final List<Long> latencies = new ArrayList<>();
    
    /**
     * 记录订单生成时间
     * 在 Kafka Producer 发送订单时调用
     * 
     * @param orderId 订单ID
     * @param timestamp 订单生成时间戳（毫秒）
     */
    public static void recordOrderGeneration(String orderId, long timestamp) {
        orderGenerationTime.put(orderId, timestamp);
    }
    
    /**
     * 记录异常检测时间，计算延迟
     * 在 Flink 检测到异常订单时调用
     * 
     * @param orderId 订单ID
     * @param detectionTime 检测到异常的时间戳（毫秒）
     */
    public static void recordAnomalyDetection(String orderId, long detectionTime) {
        Long generationTime = orderGenerationTime.get(orderId);
        if (generationTime != null) {
            long latency = detectionTime - generationTime;
            synchronized (latencies) {
                latencies.add(latency);
            }
            // 移除已处理的订单（避免内存泄漏）
            orderGenerationTime.remove(orderId);
        }
    }
    
    /**
     * 获取延迟统计
     * 
     * @return 延迟统计对象
     */
    public static LatencyStats getStats() {
        synchronized (latencies) {
            if (latencies.isEmpty()) {
                return new LatencyStats(0, 0, 0, 0, 0, 0);
            }
            
            // 排序
            List<Long> sorted = new ArrayList<>(latencies);
            sorted.sort(Long::compareTo);
            
            int size = sorted.size();
            long sum = sorted.stream().mapToLong(Long::longValue).sum();
            
            return new LatencyStats(
                size,
                sum / size,  // 平均延迟
                sorted.get(size / 2),  // P50
                sorted.get((int)(size * 0.95)),  // P95
                sorted.get((int)(size * 0.99)),  // P99
                sorted.get(size - 1)  // 最大延迟
            );
        }
    }
    
    /**
     * 清空统计数据
     * 在每次测试前调用
     */
    public static void reset() {
        orderGenerationTime.clear();
        synchronized (latencies) {
            latencies.clear();
        }
    }
    
    /**
     * 延迟统计结果
     */
    @Data
    public static class LatencyStats {
        private final int totalSamples;
        private final long avgLatency;
        private final long p50Latency;
        private final long p95Latency;
        private final long p99Latency;
        private final long maxLatency;
        
        public LatencyStats(int totalSamples, long avgLatency, long p50Latency,
                           long p95Latency, long p99Latency, long maxLatency) {
            this.totalSamples = totalSamples;
            this.avgLatency = avgLatency;
            this.p50Latency = p50Latency;
            this.p95Latency = p95Latency;
            this.p99Latency = p99Latency;
            this.maxLatency = maxLatency;
        }
        
        @Override
        public String toString() {
            return String.format(
                "延迟统计 [样本数: %d, 平均: %dms, P50: %dms, P95: %dms, P99: %dms, 最大: %dms]",
                totalSamples, avgLatency, p50Latency, p95Latency, p99Latency, maxLatency
            );
        }
        
        /**
         * 计算拦截率
         * 
         * @param totalOrders 总刷单量
         * @param ordersPerMinute 刷单速度（单/分钟）
         * @return 拦截率（0-1）
         */
        public double calculateInterceptRate(int totalOrders, int ordersPerMinute) {
            // 延迟窗口内能刷多少单
            double ordersInWindow = ordersPerMinute * (p99Latency / 60000.0);
            // 拦截的订单数
            double blockedOrders = totalOrders - ordersInWindow;
            return Math.max(0, Math.min(1, blockedOrders / totalOrders));
        }
        
        /**
         * 计算平台损失
         * 
         * @param ordersPerMinute 刷单速度（单/分钟）
         * @param orderAmount 单笔金额（元）
         * @return 平台损失（元）
         */
        public double calculateLoss(int ordersPerMinute, double orderAmount) {
            // 延迟窗口内能刷多少单
            double ordersInWindow = ordersPerMinute * (p99Latency / 60000.0);
            return ordersInWindow * orderAmount;
        }
    }
}
