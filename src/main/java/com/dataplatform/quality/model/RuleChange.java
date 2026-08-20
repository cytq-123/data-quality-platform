package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 规则变更记录
 * 用于增量更新机制
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleChange implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 变更ID */
    private Long changeId;
    
    /** 规则ID */
    private Long ruleId;
    
    /** 变更类型: ADD, UPDATE, DELETE */
    private String changeType;
    
    /** 旧值 (DELETE 时为完整规则，UPDATE 时为变更前的值) */
    private String oldValue;
    
    /** 新值 (ADD/UPDATE 时为完整规则) */
    private String newValue;
    
    /** 变更时间 */
    private Long changeTime;
    
    /** 规则版本号 */
    private Long ruleVersion;
    
    /**
     * 从 JSON 字符串创建对象
     */
    public static RuleChange fromJson(String json) {
        return com.alibaba.fastjson2.JSON.parseObject(json, RuleChange.class);
    }
    
    /**
     * 转换为 JSON 字符串
     */
    public String toJson() {
        return com.alibaba.fastjson2.JSON.toJSONString(this);
    }
}
