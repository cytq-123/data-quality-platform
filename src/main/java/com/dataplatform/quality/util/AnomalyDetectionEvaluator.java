package com.dataplatform.quality.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异常检测性能评估工具
 * 用于计算异常检测的准确率、误报率等指标
 */
public class AnomalyDetectionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionEvaluator.class);
    
    /** 真正的异常数 (True Positives) */
    private long truePositives = 0;
    
    /** 误报数 (False Positives) */
    private long falsePositives = 0;
    
    /** 漏报数 (False Negatives) */
    private long falseNegatives = 0;
    
    /** 真正的正常数 (True Negatives) */
    private long trueNegatives = 0;
    
    /**
     * 记录一个预测结果
     * 
     * @param predicted 预测是否为异常
     * @param actual 实际是否为异常
     */
    public void recordPrediction(boolean predicted, boolean actual) {
        if (predicted && actual) {
            truePositives++;  // 正确检测到异常
        } else if (predicted && !actual) {
            falsePositives++;  // 误报（错误地标记为异常）
        } else if (!predicted && actual) {
            falseNegatives++;  // 漏报（没有检测到异常）
        } else {
            trueNegatives++;   // 正确识别为正常
        }
    }
    
    /**
     * 计算准确率 (Accuracy)
     * 准确率 = (TP + TN) / (TP + TN + FP + FN)
     */
    public double getAccuracy() {
        long total = truePositives + trueNegatives + falsePositives + falseNegatives;
        if (total == 0) return 0.0;
        return (double) (truePositives + trueNegatives) / total;
    }
    
    /**
     * 计算精确率 (Precision) - 检测到的异常中有多少是真的
     * 精确率 = TP / (TP + FP)
     */
    public double getPrecision() {
        long detected = truePositives + falsePositives;
        if (detected == 0) return 0.0;
        return (double) truePositives / detected;
    }
    
    /**
     * 计算召回率 (Recall) - 真实异常中有多少被检测到
     * 召回率 = TP / (TP + FN)
     */
    public double getRecall() {
        long actual = truePositives + falseNegatives;
        if (actual == 0) return 0.0;
        return (double) truePositives / actual;
    }
    
    /**
     * 计算 F1 分数 (F1 Score)
     * F1 = 2 * (Precision * Recall) / (Precision + Recall)
     */
    public double getF1Score() {
        double precision = getPrecision();
        double recall = getRecall();
        if (precision + recall == 0) return 0.0;
        return 2.0 * (precision * recall) / (precision + recall);
    }
    
    /**
     * 计算误报率 (False Positive Rate)
     * FPR = FP / (FP + TN)
     */
    public double getFalsePositiveRate() {
        long negatives = falsePositives + trueNegatives;
        if (negatives == 0) return 0.0;
        return (double) falsePositives / negatives;
    }
    
    /**
     * 计算漏报率 (False Negative Rate)
     * FNR = FN / (FN + TP)
     */
    public double getFalseNegativeRate() {
        long positives = falseNegatives + truePositives;
        if (positives == 0) return 0.0;
        return (double) falseNegatives / positives;
    }
    
    /**
     * 计算相比基线方法的改进
     * 
     * @param baselineFPR 基线方法的误报率
     * @return 改进百分比
     */
    public double getImprovement(double baselineFPR) {
        double currentFPR = getFalsePositiveRate();
        if (baselineFPR == 0) return 0.0;
        return (baselineFPR - currentFPR) / baselineFPR * 100;
    }
    
    /**
     * 打印性能报告
     */
    public void printReport() {
        long total = truePositives + trueNegatives + falsePositives + falseNegatives;
        
        LOG.info("========== 异常检测性能评估 ==========");
        LOG.info("总样本数: {}", total);
        LOG.info("真正异常 (TP): {}", truePositives);
        LOG.info("误报 (FP): {}", falsePositives);
        LOG.info("漏报 (FN): {}", falseNegatives);
        LOG.info("真正正常 (TN): {}", trueNegatives);
        LOG.info("");
        LOG.info("准确率 (Accuracy): {}", String.format("%.2f%%", getAccuracy() * 100));
        LOG.info("精确率 (Precision): {}", String.format("%.2f%%", getPrecision() * 100));
        LOG.info("召回率 (Recall): {}", String.format("%.2f%%", getRecall() * 100));
        LOG.info("F1 分数: {}", String.format("%.4f", getF1Score()));
        LOG.info("");
        LOG.info("误报率 (FPR): {}", String.format("%.2f%%", getFalsePositiveRate() * 100));
        LOG.info("漏报率 (FNR): {}", String.format("%.2f%%", getFalseNegativeRate() * 100));
        LOG.info("=====================================");
    }
    
    /**
     * 打印对比报告
     * 
     * @param baselineName 基线方法名称
     * @param baselineFPR 基线方法的误报率
     */
    public void printComparisonReport(String baselineName, double baselineFPR) {
        printReport();
        LOG.info("");
        LOG.info("与 {} 的对比:", baselineName);
        LOG.info("基线误报率: {:.2f}%", baselineFPR * 100);
        LOG.info("当前误报率: {:.2f}%", getFalsePositiveRate() * 100);
        LOG.info("改进: {:.2f}%", getImprovement(baselineFPR));
    }
    
    // Getters
    public long getTruePositives() {
        return truePositives;
    }
    
    public long getFalsePositives() {
        return falsePositives;
    }
    
    public long getFalseNegatives() {
        return falseNegatives;
    }
    
    public long getTrueNegatives() {
        return trueNegatives;
    }
    
    public void reset() {
        truePositives = 0;
        falsePositives = 0;
        falseNegatives = 0;
        trueNegatives = 0;
    }
}
