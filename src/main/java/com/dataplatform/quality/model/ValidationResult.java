package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据校验结果
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 是否通过校验 */
    private Boolean isValid;
    
    /** 原始数据 */
    private Order order;
    
    /** 失败的规则列表 */
    private List<String> failedRules;
    
    /** 失败原因 */
    private List<String> failureReasons;
    
    /** 校验时间 */
    private Long validationTime;
    
    public ValidationResult(Order order) {
        this.order = order;
        this.isValid = true;
        this.failedRules = new ArrayList<>();
        this.failureReasons = new ArrayList<>();
        this.validationTime = System.currentTimeMillis();
    }
    
    /**
     * 添加失败规则
     */
    public void addFailure(String ruleName, String reason) {
        this.isValid = false;
        this.failedRules.add(ruleName);
        this.failureReasons.add(reason);
    }
    
    /**
     * 获取失败信息摘要
     */
    public String getFailureSummary() {
        if (isValid) {
            return "PASS";
        }
        return String.format("FAILED: %s - %s", 
            String.join(", ", failedRules),
            String.join("; ", failureReasons));
    }
}
