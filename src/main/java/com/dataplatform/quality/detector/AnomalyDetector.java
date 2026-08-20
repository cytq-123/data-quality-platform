package com.dataplatform.quality.detector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 异常检测器 - 基于统计学的 3σ 原则
 * 
 * 3σ 原则: 如果数据服从正态分布,那么:
 * - 68.27% 的数据在 μ ± σ 范围内
 * - 95.45% 的数据在 μ ± 2σ 范围内
 * - 99.73% 的数据在 μ ± 3σ 范围内
 * 
 * 超过 3σ 的数据被视为异常值
 */
public class AnomalyDetector {
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
    
    /** 移动窗口大小 (用于计算移动平均) */
    private final int windowSize;
    
    /** 异常阈值 (几倍标准差) */
    private final double threshold;
    
    /** 检测方向: BOTH(双向), HIGH(仅高值), LOW(仅低值) */
    private final DetectionDirection direction;
    
    /** 数据窗口 */
    private final Queue<Double> dataWindow;
    
    /** 当前平均值 */
    private double mean;
    
    /** 当前标准差 */
    private double stdDev;
    
    /** 数据点计数 */
    private long count;
    
    /**
     * 检测方向枚举
     */
    public enum DetectionDirection {
        BOTH,   // 双向检测 (高值和低值都检测)
        HIGH,   // 仅检测高值异常
        LOW     // 仅检测低值异常
    }
    
    /**
     * 构造函数
     * 
     * @param windowSize 移动窗口大小 (建议: 100-1000)
     * @param threshold 异常阈值 (建议: 3.0 表示 3σ)
     * @param direction 检测方向
     */
    public AnomalyDetector(int windowSize, double threshold, DetectionDirection direction) {
        this.windowSize = windowSize;
        this.threshold = threshold;
        this.direction = direction;
        this.dataWindow = new LinkedList<>();
        this.mean = 0.0;
        this.stdDev = 0.0;
        this.count = 0;
    }
    
    /**
     * 构造函数 (默认双向检测)
     * 
     * @param windowSize 移动窗口大小 (建议: 100-1000)
     * @param threshold 异常阈值 (建议: 3.0 表示 3σ)
     */
    public AnomalyDetector(int windowSize, double threshold) {
        this(windowSize, threshold, DetectionDirection.BOTH);
    }
    
    /**
     * 默认构造函数 (窗口大小=500, 阈值=3σ)
     */
    public AnomalyDetector() {
        this(500, 3.0);
    }
    
    /**
     * 检测数据点是否为异常
     * 
     * @param value 数据点
     * @return true 表示异常, false 表示正常
     */
    public boolean isAnomaly(double value) {
        // 前 windowSize 个数据点用于建立基线,不判定为异常
        if (count < windowSize) {
            addDataPoint(value);
            return false;
        }
        
        // 计算 Z-Score (标准分数)
        double zScore = calculateZScore(value);
        
        // 根据检测方向判断是否异常
        boolean isAnomaly = false;
        switch (direction) {
            case BOTH:
                // 双向检测: 高值或低值都算异常
                isAnomaly = Math.abs(zScore) > threshold;
                break;
            case HIGH:
                // 仅检测高值异常: zScore > threshold
                isAnomaly = zScore > threshold;
                break;
            case LOW:
                // 仅检测低值异常: zScore < -threshold
                isAnomaly = zScore < -threshold;
                break;
        }
        
        if (isAnomaly) {
            LOG.warn("Anomaly detected: value={}, mean={}, stdDev={}, zScore={}, direction={}", 
                value, mean, stdDev, zScore, direction);
        }
        
        // 更新窗口
        addDataPoint(value);
        
        return isAnomaly;
    }
    
    /**
     * 添加数据点并更新统计信息
     */
    private void addDataPoint(double value) {
        dataWindow.offer(value);
        count++;
        
        // 如果窗口满了,移除最老的数据点
        if (dataWindow.size() > windowSize) {
            dataWindow.poll();
        }
        
        // 重新计算均值和标准差
        calculateStatistics();
    }
    
    /**
     * 计算 Z-Score (标准分数)
     * Z-Score = (value - mean) / stdDev
     */
    private double calculateZScore(double value) {
        if (stdDev == 0) {
            return 0.0;
        }
        return (value - mean) / stdDev;
    }
    
    /**
     * 计算均值和标准差
     */
    private void calculateStatistics() {
        if (dataWindow.isEmpty()) {
            mean = 0.0;
            stdDev = 0.0;
            return;
        }
        
        // 计算均值
        double sum = 0.0;
        for (double value : dataWindow) {
            sum += value;
        }
        mean = sum / dataWindow.size();
        
        // 计算标准差
        double variance = 0.0;
        for (double value : dataWindow) {
            variance += Math.pow(value - mean, 2);
        }
        variance /= dataWindow.size();
        stdDev = Math.sqrt(variance);
    }
    
    /**
     * 获取异常分数 (Z-Score 的绝对值)
     */
    public double getAnomalyScore(double value) {
        if (count < windowSize) {
            return 0.0;
        }
        return Math.abs(calculateZScore(value));
    }
    
    /**
     * 获取异常等级
     * 
     * @return 1: 轻微异常 (2σ-3σ), 2: 中度异常 (3σ-4σ), 3: 严重异常 (>4σ)
     */
    public int getAnomalyLevel(double value) {
        double score = getAnomalyScore(value);
        
        if (score > 4.0) {
            return 3; // 严重异常
        } else if (score > 3.0) {
            return 2; // 中度异常
        } else if (score > 2.0) {
            return 1; // 轻微异常
        } else {
            return 0; // 正常
        }
    }
    
    /**
     * 重置检测器
     */
    public void reset() {
        dataWindow.clear();
        mean = 0.0;
        stdDev = 0.0;
        count = 0;
        LOG.info("Anomaly detector reset");
    }
    
    // Getters
    public double getMean() {
        return mean;
    }
    
    public double getStdDev() {
        return stdDev;
    }
    
    public long getCount() {
        return count;
    }
    
    public int getWindowSize() {
        return windowSize;
    }
    
    public double getThreshold() {
        return threshold;
    }
    
    public DetectionDirection getDirection() {
        return direction;
    }
}
