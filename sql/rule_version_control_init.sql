-- ============================================================================
-- 规则版本控制和增量更新初始化脚本
-- 支持版本号检查和增量更新机制
-- ============================================================================

-- 1. 为 quality_rules 表添加版本号字段（如果不存在）
-- 使用兼容 MySQL 5.7 的写法
SET @col_exists = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'quality_rules'
    AND COLUMN_NAME = 'rule_version'
);

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE quality_rules ADD COLUMN rule_version BIGINT DEFAULT 1 COMMENT "规则版本号"',
    'SELECT "Column rule_version already exists"'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2. 为 quality_rules 表添加索引（如果不存在）
SET @idx_exists = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'quality_rules'
    AND INDEX_NAME = 'idx_rule_version'
);

SET @sql = IF(@idx_exists = 0,
    'ALTER TABLE quality_rules ADD INDEX idx_rule_version (rule_version)',
    'SELECT "Index idx_rule_version already exists"'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists2 = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'quality_rules'
    AND INDEX_NAME = 'idx_enabled_version'
);

SET @sql2 = IF(@idx_exists2 = 0,
    'ALTER TABLE quality_rules ADD INDEX idx_enabled_version (enabled, rule_version)',
    'SELECT "Index idx_enabled_version already exists"'
);

PREPARE stmt2 FROM @sql2;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

-- 3. 创建规则变更日志表
CREATE TABLE IF NOT EXISTS rule_changelog (
    change_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '变更ID',
    rule_id BIGINT NOT NULL COMMENT '规则ID',
    change_type VARCHAR(20) NOT NULL COMMENT '变更类型: ADD, UPDATE, DELETE',
    old_value LONGTEXT COMMENT '旧值 (DELETE时为完整规则，UPDATE时为变更前的值)',
    new_value LONGTEXT COMMENT '新值 (ADD/UPDATE时为完整规则)',
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '变更时间',
    rule_version BIGINT NOT NULL COMMENT '规则版本号',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    
    INDEX idx_rule_id (rule_id),
    INDEX idx_rule_version (rule_version),
    INDEX idx_change_type (change_type),
    INDEX idx_change_time (change_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='规则变更日志表';

-- 4. 创建触发器：当规则被添加时，记录变更日志
DELIMITER $$

DROP TRIGGER IF EXISTS trg_rule_insert$$

CREATE TRIGGER trg_rule_insert
AFTER INSERT ON quality_rules
FOR EACH ROW
BEGIN
    -- 获取最新的版本号
    SET @max_version = (SELECT COALESCE(MAX(rule_version), 0) FROM quality_rules WHERE rule_id != NEW.rule_id);
    SET @new_version = @max_version + 1;
    
    -- 更新新插入的规则的版本号
    UPDATE quality_rules SET rule_version = @new_version WHERE rule_id = NEW.rule_id;
    
    -- 记录变更日志
    INSERT INTO rule_changelog (rule_id, change_type, new_value, rule_version)
    VALUES (
        NEW.rule_id,
        'ADD',
        JSON_OBJECT(
            'ruleId', NEW.rule_id,
            'ruleName', NEW.rule_name,
            'ruleType', NEW.rule_type,
            'field', NEW.field,
            'condition', NEW.condition,
            'action', NEW.action,
            'priority', NEW.priority,
            'enabled', NEW.enabled,
            'description', NEW.description,
            'createTime', UNIX_TIMESTAMP(NEW.create_time) * 1000,
            'updateTime', UNIX_TIMESTAMP(NEW.update_time) * 1000,
            'ruleVersion', @new_version
        ),
        @new_version
    );
END$$

DELIMITER ;

-- 5. 创建触发器：当规则被更新时，记录变更日志
DELIMITER $$

DROP TRIGGER IF EXISTS trg_rule_update$$

CREATE TRIGGER trg_rule_update
AFTER UPDATE ON quality_rules
FOR EACH ROW
BEGIN
    -- 只有当规则内容实际变化时才记录
    IF (
        OLD.rule_name != NEW.rule_name OR
        OLD.rule_type != NEW.rule_type OR
        OLD.field != NEW.field OR
        OLD.condition != NEW.condition OR
        OLD.action != NEW.action OR
        OLD.priority != NEW.priority OR
        OLD.enabled != NEW.enabled OR
        OLD.description != NEW.description
    ) THEN
        -- 获取最新的版本号
        SET @max_version = (SELECT COALESCE(MAX(rule_version), 0) FROM rule_changelog);
        SET @new_version = @max_version + 1;
        
        -- 更新规则的版本号
        UPDATE quality_rules SET rule_version = @new_version WHERE rule_id = NEW.rule_id;
        
        -- 记录变更日志
        INSERT INTO rule_changelog (rule_id, change_type, old_value, new_value, rule_version)
        VALUES (
            NEW.rule_id,
            'UPDATE',
            JSON_OBJECT(
                'ruleId', OLD.rule_id,
                'ruleName', OLD.rule_name,
                'ruleType', OLD.rule_type,
                'field', OLD.field,
                'condition', OLD.condition,
                'action', OLD.action,
                'priority', OLD.priority,
                'enabled', OLD.enabled,
                'description', OLD.description,
                'createTime', UNIX_TIMESTAMP(OLD.create_time) * 1000,
                'updateTime', UNIX_TIMESTAMP(OLD.update_time) * 1000,
                'ruleVersion', OLD.rule_version
            ),
            JSON_OBJECT(
                'ruleId', NEW.rule_id,
                'ruleName', NEW.rule_name,
                'ruleType', NEW.rule_type,
                'field', NEW.field,
                'condition', NEW.condition,
                'action', NEW.action,
                'priority', NEW.priority,
                'enabled', NEW.enabled,
                'description', NEW.description,
                'createTime', UNIX_TIMESTAMP(NEW.create_time) * 1000,
                'updateTime', UNIX_TIMESTAMP(NEW.update_time) * 1000,
                'ruleVersion', @new_version
            ),
            @new_version
        );
    END IF;
END$$

DELIMITER ;

-- 6. 创建触发器：当规则被删除时，记录变更日志
DELIMITER $$

DROP TRIGGER IF EXISTS trg_rule_delete$$

CREATE TRIGGER trg_rule_delete
AFTER DELETE ON quality_rules
FOR EACH ROW
BEGIN
    -- 获取最新的版本号
    SET @max_version = (SELECT COALESCE(MAX(rule_version), 0) FROM rule_changelog);
    SET @new_version = @max_version + 1;
    
    -- 记录变更日志
    INSERT INTO rule_changelog (rule_id, change_type, old_value, rule_version)
    VALUES (
        OLD.rule_id,
        'DELETE',
        JSON_OBJECT(
            'ruleId', OLD.rule_id,
            'ruleName', OLD.rule_name,
            'ruleType', OLD.rule_type,
            'field', OLD.field,
            'condition', OLD.condition,
            'action', OLD.action,
            'priority', OLD.priority,
            'enabled', OLD.enabled,
            'description', OLD.description,
            'createTime', UNIX_TIMESTAMP(OLD.create_time) * 1000,
            'updateTime', UNIX_TIMESTAMP(OLD.update_time) * 1000,
            'ruleVersion', OLD.rule_version
        ),
        @new_version
    );
END$$

DELIMITER ;

-- 7. 创建视图：查看规则变更历史
CREATE OR REPLACE VIEW v_rule_changelog AS
SELECT
    change_id,
    rule_id,
    change_type,
    rule_version,
    change_time,
    DATE_FORMAT(change_time, '%Y-%m-%d %H:%i:%s') as change_time_str,
    CASE
        WHEN change_type = 'ADD' THEN '新增'
        WHEN change_type = 'UPDATE' THEN '更新'
        WHEN change_type = 'DELETE' THEN '删除'
        ELSE change_type
    END as change_type_cn
FROM rule_changelog
ORDER BY rule_version DESC, change_id DESC;

-- 8. 创建存储过程：获取自指定版本以来的规则变更
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_get_rule_changes_since$$

CREATE PROCEDURE sp_get_rule_changes_since(
    IN p_since_version BIGINT
)
BEGIN
    SELECT
        change_id,
        rule_id,
        change_type,
        old_value,
        new_value,
        UNIX_TIMESTAMP(change_time) * 1000 as change_time,
        rule_version
    FROM rule_changelog
    WHERE rule_version > p_since_version
    ORDER BY rule_version ASC, change_id ASC;
END$$

DELIMITER ;

-- 9. 创建存储过程：获取最新的规则版本号
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_get_latest_rule_version$$

CREATE PROCEDURE sp_get_latest_rule_version()
BEGIN
    SELECT COALESCE(MAX(rule_version), 0) as latest_version
    FROM quality_rules
    WHERE enabled = 1;
END$$

DELIMITER ;

-- 10. 创建存储过程：清理旧的变更日志（保留最近 30 天）
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_cleanup_old_changelog$$

CREATE PROCEDURE sp_cleanup_old_changelog()
BEGIN
    DELETE FROM rule_changelog
    WHERE change_time < DATE_SUB(NOW(), INTERVAL 30 DAY);
    
    SELECT ROW_COUNT() as deleted_rows;
END$$

DELIMITER ;

-- 11. 初始化现有规则的版本号（如果还没有）
UPDATE quality_rules
SET rule_version = rule_id
WHERE rule_version IS NULL OR rule_version = 0;

-- 12. 创建索引以优化查询性能（如果不存在）
SET @idx_exists3 = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'rule_changelog'
    AND INDEX_NAME = 'idx_rule_version_change_id'
);

SET @sql3 = IF(@idx_exists3 = 0,
    'ALTER TABLE rule_changelog ADD INDEX idx_rule_version_change_id (rule_version, change_id)',
    'SELECT "Index idx_rule_version_change_id already exists"'
);

PREPARE stmt3 FROM @sql3;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @idx_exists4 = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'rule_changelog'
    AND INDEX_NAME = 'idx_change_time_rule_version'
);

SET @sql4 = IF(@idx_exists4 = 0,
    'ALTER TABLE rule_changelog ADD INDEX idx_change_time_rule_version (change_time, rule_version)',
    'SELECT "Index idx_change_time_rule_version already exists"'
);

PREPARE stmt4 FROM @sql4;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;

-- ============================================================================
-- 性能优化建议
-- ============================================================================

-- 定期清理旧的变更日志（建议每天执行一次）
-- CALL sp_cleanup_old_changelog();

-- 查看规则变更历史
-- SELECT * FROM v_rule_changelog LIMIT 100;

-- 获取自指定版本以来的规则变更
-- CALL sp_get_rule_changes_since(0);

-- 获取最新的规则版本号
-- CALL sp_get_latest_rule_version();

-- ============================================================================
-- 测试数据
-- ============================================================================

-- 插入测试规则
INSERT INTO quality_rules (rule_name, rule_type, field, `condition`, action, priority, enabled, description)
VALUES
    ('订单金额校验', 'RANGE', 'order_amount', '0,100000', 'REJECT', 1, 1, '订单金额必须在0-100000之间'),
    ('用户ID非空校验', 'NOT_NULL', 'user_id', '', 'REJECT', 2, 1, '用户ID不能为空'),
    ('手机号格式校验', 'REGEX', 'phone', '^1[3-9][0-9]{9}$', 'ALERT', 3, 1, '手机号格式校验')
ON DUPLICATE KEY UPDATE updated_at = NOW();

-- ============================================================================
-- 验证脚本（需要在 MySQL 客户端中手动执行）
-- ============================================================================

-- 验证规则表
-- SELECT COUNT(*) as total_rules FROM quality_rules;
-- SELECT * FROM quality_rules LIMIT 5;

-- 验证变更日志表
-- SELECT COUNT(*) as total_changes FROM rule_changelog;
-- SELECT * FROM rule_changelog LIMIT 5;

-- 验证视图
-- SELECT * FROM v_rule_changelog LIMIT 10;

-- 验证最新版本号
-- CALL sp_get_latest_rule_version();

-- ============================================================================
-- 完成
-- ============================================================================
