package com.dataplatform.quality.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简化版恶意刷单测试
 * 
 * 用于快速演示延迟与刷单量的关系
 */
public class SimpleFraudTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFraudTest.class);
    
    public static void main(String[] args) {
        LOG.info("=== 抖音电商恶意刷单检测 - 延迟测试 ===\n");
        
        // 测试参数
        int ordersPerMinute = 10;  // 羊毛党刷单速度：10单/分钟
        double orderAmount = 100.0;  // 每单金额：100元
        
        // 测试不同延迟场景
        testScenario("T+1 离线检测", 24 * 60 * 60 * 1000, ordersPerMinute, orderAmount);
        testScenario("1小时延迟", 60 * 60 * 1000, ordersPerMinute, orderAmount);
        testScenario("10分钟延迟", 10 * 60 * 1000, ordersPerMinute, orderAmount);
        testScenario("1分钟延迟", 60 * 1000, ordersPerMinute, orderAmount);
        testScenario("5秒延迟（优化后）", 5 * 1000, ordersPerMinute, orderAmount);
        testScenario("1秒延迟（极致优化）", 1 * 1000, ordersPerMinute, orderAmount);
        testScenario("100ms延迟（理论极限）", 100, ordersPerMinute, orderAmount);
        
        LOG.info("\n=== 测试完成 ===");
    }
    
    /**
     * 测试单个延迟场景
     */
    private static void testScenario(String scenarioName, long delayMs, 
                                    int ordersPerMinute, double orderAmount) {
        
        LOG.info("\n----- {} -----", scenarioName);
        LOG.info("检测延迟: {}ms ({})", delayMs, formatDelay(delayMs));
        
        // 计算延迟窗口内能刷多少单
        double ordersInWindow = ordersPerMinute * (delayMs / 60000.0);
        LOG.info("延迟窗口内能刷: {:.2f} 单", ordersInWindow);
        
        // 计算平台损失
        double loss = ordersInWindow * orderAmount;
        LOG.info("平台单次损失: {:.2f} 元", loss);
        
        // 计算年损失（假设每月攻击10次）
        double yearLoss = loss * 10 * 12;
        LOG.info("年损失: {:.2f} 元 ({})", yearLoss, formatMoney(yearLoss));
        
        // 计算拦截率
        int totalAttempt = 100;  // 羊毛党尝试刷100单
        double blockedOrders = Math.max(0, totalAttempt - ordersInWindow);
        double interceptRate = blockedOrders / totalAttempt * 100;
        LOG.info("拦截率: {:.2f}% ({:.0f}/{}单被拦截)", 
                 interceptRate, blockedOrders, totalAttempt);
    }
    
    /**
     * 格式化延迟显示
     */
    private static String formatDelay(long delayMs) {
        if (delayMs >= 24 * 60 * 60 * 1000) {
            return String.format("%.1f天", delayMs / (24 * 60 * 60 * 1000.0));
        } else if (delayMs >= 60 * 60 * 1000) {
            return String.format("%.1f小时", delayMs / (60 * 60 * 1000.0));
        } else if (delayMs >= 60 * 1000) {
            return String.format("%.1f分钟", delayMs / (60 * 1000.0));
        } else if (delayMs >= 1000) {
            return String.format("%.1f秒", delayMs / 1000.0);
        } else {
            return delayMs + "毫秒";
        }
    }
    
    /**
     * 格式化金额显示
     */
    private static String formatMoney(double amount) {
        if (amount >= 100_000_000) {
            return String.format("%.2f亿", amount / 100_000_000);
        } else if (amount >= 10_000) {
            return String.format("%.2f万", amount / 10_000);
        } else {
            return String.format("%.2f", amount);
        }
    }
}
