package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 数据质量规则模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QualityRule implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 规则ID */
    private Long ruleId;
    
    /** 规则名称 */
    private String ruleName;
    
    /** 规则类型: RANGE, NOT_NULL, REGEX, CUSTOM */
    private String ruleType;
    
    /** 校验字段 */
    private String field;
    
    /** 校验条件 */
    private String condition;
    
    /** 处理动作: REJECT(拒绝), ALERT(告警), PASS(通过) */
    private String action;
    
    /** 规则优先级 (数字越小优先级越高) */
    private Integer priority;
    
    /** 是否启用 */
    private Boolean enabled;
    
    /** 规则描述 */
    private String description;
    
    /** 创建时间 */
    private Long createTime;
    
    /** 更新时间 */
    private Long updateTime;
    
    /** 规则版本号 (用于增量更新) */
    private Long ruleVersion;
    
    // ========================================
    // 3σ 异常检测配置 (可选)
    // ========================================
    
    /** 是否启用异常检测 */
    private Boolean anomalyDetectionEnabled;
    
    /** 异常检测字段 (如: orderAmount, quantity) */
    private String detectionField;
    
    /** 移动窗口大小 (默认: 500) */
    private Integer windowSize;
    
    /** 异常阈值系数 (默认: 3.0 表示 3σ) */
    private Double sigmaThreshold;
    
    /** 异常检测方向: BOTH(双向), HIGH(仅高值), LOW(仅低值) */
    private String detectionDirection;
    
    /**
     * 从 JSON 字符串创建规则对象
     */
    public static QualityRule fromJson(String json) {
        return com.alibaba.fastjson2.JSON.parseObject(json, QualityRule.class);
    }
    
    /**
     * 转换为 JSON 字符串
     */
    public String toJson() {
        return com.alibaba.fastjson2.JSON.toJSONString(this);
    }
}
