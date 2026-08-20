package com.dataplatform.quality.rule;

import com.dataplatform.quality.model.QualityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 规则加载器 - 从 MySQL 加载规则
 */
public class RuleLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RuleLoader.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    public RuleLoader(Properties config) {
        this.jdbcUrl = config.getProperty("mysql.url");
        this.username = config.getProperty("mysql.username");
        this.password = config.getProperty("mysql.password");
    }
    
    public RuleLoader(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    /**
     * 加载所有规则
     */
    public List<QualityRule> loadAllRules() {
        List<QualityRule> rules = new ArrayList<>();
        
        String sql = "SELECT rule_id, rule_name, rule_type, field, `condition`, " +
                     "action, priority, enabled, description, create_time, update_time " +
                     "FROM quality_rules WHERE enabled = 1 ORDER BY priority ASC";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                QualityRule rule = new QualityRule();
                rule.setRuleId(rs.getLong("rule_id"));
                rule.setRuleName(rs.getString("rule_name"));
                rule.setRuleType(rs.getString("rule_type"));
                rule.setField(rs.getString("field"));
                rule.setCondition(rs.getString("condition"));
                rule.setAction(rs.getString("action"));
                rule.setPriority(rs.getInt("priority"));
                rule.setEnabled(rs.getBoolean("enabled"));
                rule.setDescription(rs.getString("description"));
                rule.setCreateTime(rs.getTimestamp("create_time").getTime());
                rule.setUpdateTime(rs.getTimestamp("update_time").getTime());
                // rule_version 字段不存在，跳过
                // rule.setRuleVersion(rs.getLong("rule_version"));
                
                rules.add(rule);
                
                LOG.debug("Loaded rule: id={}, name={}, type={}, field={}, condition={}, enabled={}", 
                    rule.getRuleId(), rule.getRuleName(), rule.getRuleType(), 
                    rule.getField(), rule.getCondition(), rule.getEnabled());
            }
            
            LOG.info("Loaded {} rules from database", rules.size());
            
        } catch (SQLException e) {
            LOG.error("Error loading rules from database", e);
        }
        
        return rules;
    }
    
    /**
     * 根据规则ID加载规则
     */
    public QualityRule loadRuleById(Long ruleId) {
        String sql = "SELECT rule_id, rule_name, rule_type, field, `condition`, " +
                     "action, priority, enabled, description, create_time, update_time " +
                     "FROM quality_rules WHERE rule_id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setLong(1, ruleId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    QualityRule rule = new QualityRule();
                    rule.setRuleId(rs.getLong("rule_id"));
                    rule.setRuleName(rs.getString("rule_name"));
                    rule.setRuleType(rs.getString("rule_type"));
                    rule.setField(rs.getString("field"));
                    rule.setCondition(rs.getString("condition"));
                    rule.setAction(rs.getString("action"));
                    rule.setPriority(rs.getInt("priority"));
                    rule.setEnabled(rs.getBoolean("enabled"));
                    rule.setDescription(rs.getString("description"));
                    rule.setCreateTime(rs.getTimestamp("create_time").getTime());
                    rule.setUpdateTime(rs.getTimestamp("update_time").getTime());
                    
                    return rule;
                }
            }
            
        } catch (SQLException e) {
            LOG.error("Error loading rule by id: {}", ruleId, e);
        }
        
        return null;
    }
    
    /**
     * 保存规则
     */
    public boolean saveRule(QualityRule rule) {
        String sql = "INSERT INTO quality_rules (rule_name, rule_type, field, `condition`, " +
                     "action, priority, enabled, description, create_time, update_time) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            stmt.setString(1, rule.getRuleName());
            stmt.setString(2, rule.getRuleType());
            stmt.setString(3, rule.getField());
            stmt.setString(4, rule.getCondition());
            stmt.setString(5, rule.getAction());
            stmt.setInt(6, rule.getPriority());
            stmt.setBoolean(7, rule.getEnabled());
            stmt.setString(8, rule.getDescription());
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        rule.setRuleId(generatedKeys.getLong(1));
                    }
                }
                LOG.info("Rule saved: {}", rule.getRuleName());
                return true;
            }
            
        } catch (SQLException e) {
            LOG.error("Error saving rule: {}", rule.getRuleName(), e);
        }
        
        return false;
    }
    
    /**
     * 更新规则
     */
    public boolean updateRule(QualityRule rule) {
        String sql = "UPDATE quality_rules SET rule_name = ?, rule_type = ?, field = ?, " +
                     "`condition` = ?, action = ?, priority = ?, enabled = ?, " +
                     "description = ?, update_time = NOW() WHERE rule_id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, rule.getRuleName());
            stmt.setString(2, rule.getRuleType());
            stmt.setString(3, rule.getField());
            stmt.setString(4, rule.getCondition());
            stmt.setString(5, rule.getAction());
            stmt.setInt(6, rule.getPriority());
            stmt.setBoolean(7, rule.getEnabled());
            stmt.setString(8, rule.getDescription());
            stmt.setLong(9, rule.getRuleId());
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                LOG.info("Rule updated: {}", rule.getRuleName());
                return true;
            }
            
        } catch (SQLException e) {
            LOG.error("Error updating rule: {}", rule.getRuleName(), e);
        }
        
        return false;
    }
    
    /**
     * 删除规则
     */
    public boolean deleteRule(Long ruleId) {
        String sql = "DELETE FROM quality_rules WHERE rule_id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setLong(1, ruleId);
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                LOG.info("Rule deleted: {}", ruleId);
                return true;
            }
            
        } catch (SQLException e) {
            LOG.error("Error deleting rule: {}", ruleId, e);
        }
        
        return false;
    }
    
    /**
     * 获取最新的规则版本号
     */
    public Long getLatestRuleVersion() {
        String sql = "SELECT MAX(rule_version) as max_version FROM quality_rules WHERE enabled = 1";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                Long version = rs.getLong("max_version");
                LOG.debug("Latest rule version: {}", version);
                return version;
            }
            
        } catch (SQLException e) {
            LOG.error("Error getting latest rule version", e);
        }
        
        return 0L;
    }
    
    /**
     * 获取自指定版本以来的规则变更
     * 
     * @param sinceVersion 起始版本号
     * @return 规则变更列表
     */
    public List<com.dataplatform.quality.model.RuleChange> getChangesSince(Long sinceVersion) {
        List<com.dataplatform.quality.model.RuleChange> changes = new ArrayList<>();
        
        String sql = "SELECT change_id, rule_id, change_type, old_value, new_value, " +
                     "change_time, rule_version FROM rule_changelog " +
                     "WHERE rule_version > ? ORDER BY rule_version ASC, change_id ASC";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setLong(1, sinceVersion);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    com.dataplatform.quality.model.RuleChange change = 
                        new com.dataplatform.quality.model.RuleChange();
                    change.setChangeId(rs.getLong("change_id"));
                    change.setRuleId(rs.getLong("rule_id"));
                    change.setChangeType(rs.getString("change_type"));
                    change.setOldValue(rs.getString("old_value"));
                    change.setNewValue(rs.getString("new_value"));
                    change.setChangeTime(rs.getTimestamp("change_time").getTime());
                    change.setRuleVersion(rs.getLong("rule_version"));
                    
                    changes.add(change);
                }
            }
            
            LOG.info("Loaded {} rule changes since version {}", changes.size(), sinceVersion);
            
        } catch (SQLException e) {
            LOG.error("Error loading rule changes since version: {}", sinceVersion, e);
        }
        
        return changes;
    }
    
    /**
     * 获取数据库连接
     */
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
