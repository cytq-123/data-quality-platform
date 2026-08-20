package com.dataplatform.quality.model;

import java.util.List;

/**
 * 规则备份模型
 * 
 * 包含版本号、时间戳和规则列表
 */
public class RuleBackup {
    /** 备份版本号（对应规则的最大版本号）*/
    private Long version;
    
    /** 备份时间戳（毫秒）*/
    private Long timestamp;
    
    /** 规则列表 */
    private List<QualityRule> rules;
    
    /** 备份来源（MYSQL / REDIS / CACHE）*/
    private String source;
    
    /** 规则数量 */
    private Integer ruleCount;
    
    public RuleBackup() {
    }
    
    public RuleBackup(Long version, Long timestamp, List<QualityRule> rules, String source) {
        this.version = version;
        this.timestamp = timestamp;
        this.rules = rules;
        this.source = source;
        this.ruleCount = rules != null ? rules.size() : 0;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public List<QualityRule> getRules() {
        return rules;
    }
    
    public void setRules(List<QualityRule> rules) {
        this.rules = rules;
        this.ruleCount = rules != null ? rules.size() : 0;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public Integer getRuleCount() {
        return ruleCount;
    }
    
    public void setRuleCount(Integer ruleCount) {
        this.ruleCount = ruleCount;
    }
    
    /**
     * 获取备份年龄（秒）
     */
    public long getAgeInSeconds() {
        return (System.currentTimeMillis() - timestamp) / 1000;
    }
    
    /**
     * 获取备份年龄（分钟）
     */
    public long getAgeInMinutes() {
        return getAgeInSeconds() / 60;
    }
    
    /**
     * 获取备份年龄（小时）
     */
    public long getAgeInHours() {
        return getAgeInMinutes() / 60;
    }
    
    @Override
    public String toString() {
        return "RuleBackup{" +
                "version=" + version +
                ", timestamp=" + timestamp +
                ", ruleCount=" + ruleCount +
                ", source='" + source + '\'' +
                ", ageInMinutes=" + getAgeInMinutes() +
                '}';
    }
}
