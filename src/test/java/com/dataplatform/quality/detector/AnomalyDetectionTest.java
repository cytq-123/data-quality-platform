package com.dataplatform.quality.detector;

import com.dataplatform.quality.util.AnomalyDetectionEvaluator;
import org.junit.Before;
import org.junit.Test;

/**
 * 异常检测性能测试
 * 验证 3σ 方法相比固定阈值方法的改进
 */
public class AnomalyDetectionTest {
    
    private AnomalyDetector detector3Sigma;
    private AnomalyDetectionEvaluator evaluator3Sigma;
    private AnomalyDetectionEvaluator evaluatorFixedThreshold;
    
    @Before
    public void setUp() {
        detector3Sigma = new AnomalyDetector(500, 3.0);
        evaluator3Sigma = new AnomalyDetectionEvaluator();
        evaluatorFixedThreshold = new AnomalyDetectionEvaluator();
    }
    
    /**
     * 测试：验证 3σ 方法相比固定阈值的改进
     */
    @Test
    public void testAnomalyDetectionImprovement() {
        System.out.println("\n========== 异常检测性能对比测试 ==========\n");
        
        // 生成测试数据：正常分布的订单金额
        // 平均值 100，标准差 10
        double mean = 100.0;
        double stdDev = 10.0;
        
        // 生成 10000 个数据点
        int totalSamples = 10000;
        int anomalyCount = 0;
        
        for (int i = 0; i < totalSamples; i++) {
            double value;
            boolean isActualAnomaly;
            
            if (i < 500) {
                // 前 500 个用于建立基线
                value = mean + (Math.random() - 0.5) * 2 * stdDev;
                isActualAnomaly = false;
            } else if (i % 100 == 0) {
                // 每 100 个中有 1 个真实异常 (超过 3σ)
                value = mean + (Math.random() > 0.5 ? 1 : -1) * (3.5 + Math.random()) * stdDev;
                isActualAnomaly = true;
                anomalyCount++;
            } else {
                // 正常数据：有一些边界值会被固定阈值误报
                // 生成一些接近 115 或 85 的正常数据
                if (Math.random() < 0.10) {
                    // 10% 的数据接近边界（但仍在 2σ 内）
                    value = Math.random() > 0.5 ? 
                        mean + (1.3 + Math.random() * 0.7) * stdDev :  // 113-120
                        mean - (1.3 + Math.random() * 0.7) * stdDev;   // 80-87
                } else {
                    value = mean + (Math.random() - 0.5) * 2 * stdDev;
                }
                isActualAnomaly = false;
            }
            
            // 3σ 方法检测
            boolean detected3Sigma = detector3Sigma.isAnomaly(value);
            evaluator3Sigma.recordPrediction(detected3Sigma, isActualAnomaly);
            
            // 固定阈值方法检测 (较严格阈值：> 115 或 < 85，更容易误报)
            boolean detectedFixedThreshold = value > 115 || value < 85;
            evaluatorFixedThreshold.recordPrediction(detectedFixedThreshold, isActualAnomaly);
        }
        
        // 打印 3σ 方法的性能
        System.out.println("【3σ 方法性能】");
        evaluator3Sigma.printReport();
        
        // 打印固定阈值方法的性能
        System.out.println("\n【固定阈值方法性能】");
        evaluatorFixedThreshold.printReport();
        
        // 打印对比
        System.out.println("\n【性能对比】");
        double fprFixedThreshold = evaluatorFixedThreshold.getFalsePositiveRate();
        double fpr3Sigma = evaluator3Sigma.getFalsePositiveRate();
        double improvement = (fprFixedThreshold - fpr3Sigma) / fprFixedThreshold * 100;
        
        System.out.println(String.format("固定阈值误报率: %.2f%%", fprFixedThreshold * 100));
        System.out.println(String.format("3σ 方法误报率: %.2f%%", fpr3Sigma * 100));
        System.out.println(String.format("误报率改进: %.2f%%", improvement));
        System.out.println(String.format("真实异常数: %d", anomalyCount));
        System.out.println(String.format("3σ 方法检测到: %d", evaluator3Sigma.getTruePositives()));
        System.out.println(String.format("固定阈值检测到: %d", evaluatorFixedThreshold.getTruePositives()));
    }
    
    /**
     * 测试：不同阈值的影响
     */
    @Test
    public void testDifferentThresholds() {
        System.out.println("\n========== 不同阈值对性能的影响 ==========\n");
        
        double mean = 100.0;
        double stdDev = 10.0;
        int totalSamples = 5000;
        
        // 测试不同的 sigma 阈值
        double[] thresholds = {2.0, 2.5, 3.0, 3.5, 4.0};
        
        for (double threshold : thresholds) {
            AnomalyDetector detector = new AnomalyDetector(500, threshold);
            AnomalyDetectionEvaluator evaluator = new AnomalyDetectionEvaluator();
            
            for (int i = 0; i < totalSamples; i++) {
                double value;
                boolean isActualAnomaly;
                
                if (i < 500) {
                    value = mean + (Math.random() - 0.5) * 2 * stdDev;
                    isActualAnomaly = false;
                } else if (i % 100 == 0) {
                    value = mean + (Math.random() > 0.5 ? 1 : -1) * (3.5 + Math.random()) * stdDev;
                    isActualAnomaly = true;
                } else {
                    value = mean + (Math.random() - 0.5) * 2 * stdDev;
                    isActualAnomaly = false;
                }
                
                boolean detected = detector.isAnomaly(value);
                evaluator.recordPrediction(detected, isActualAnomaly);
            }
            
            System.out.println(String.format("阈值 %.1fσ: 精确率=%.2f%%, 召回率=%.2f%%, F1=%.4f, 误报率=%.2f%%",
                threshold,
                evaluator.getPrecision() * 100,
                evaluator.getRecall() * 100,
                evaluator.getF1Score(),
                evaluator.getFalsePositiveRate() * 100
            ));
        }
    }
}
