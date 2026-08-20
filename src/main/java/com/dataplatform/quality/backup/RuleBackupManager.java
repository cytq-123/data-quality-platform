package com.dataplatform.quality.backup;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.model.RuleBackup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 规则备份管理器
 * 
 * 功能：
 * 1. 保存规则到本地备份文件（带版本号和时间戳）
 * 2. 从本地备份文件加载规则
 * 3. 保留最近 3 个版本的备份文件
 * 4. 自动清理旧备份
 */
public class RuleBackupManager {
    private static final Logger LOG = LoggerFactory.getLogger(RuleBackupManager.class);
    
    /** 备份文件目录 */
    private static final String BACKUP_DIR = "/tmp/quality_rules_backup";
    
    /** 备份文件前缀 */
    private static final String BACKUP_FILE_PREFIX = "rules_backup_";
    
    /** 备份文件后缀 */
    private static final String BACKUP_FILE_SUFFIX = ".json";
    
    /** 保留的备份文件数量 */
    private static final int MAX_BACKUP_COUNT = 3;
    
    /**
     * 保存规则到备份文件
     * 
     * 文件名格式: rules_backup_{version}_{timestamp}.json
     * 例如: rules_backup_1234_1704067200000.json
     * 
     * @param rules 规则列表
     * @param version 规则版本号
     * @param source 备份来源（MYSQL / REDIS / CACHE）
     * @return 是否保存成功
     */
    public static boolean saveBackup(List<QualityRule> rules, Long version, String source) {
        try {
            // 创建备份目录
            File backupDir = new File(BACKUP_DIR);
            if (!backupDir.exists()) {
                backupDir.mkdirs();
                LOG.info("Created backup directory: {}", BACKUP_DIR);
            }
            
            // 创建备份对象
            long timestamp = System.currentTimeMillis();
            RuleBackup backup = new RuleBackup(version, timestamp, rules, source);
            
            // 生成备份文件名
            String fileName = String.format("%s%d_%d%s", 
                BACKUP_FILE_PREFIX, version, timestamp, BACKUP_FILE_SUFFIX);
            Path filePath = Paths.get(BACKUP_DIR, fileName);
            
            // 保存到文件
            String json = JSON.toJSONString(backup, JSONWriter.Feature.PrettyFormat);
            Files.write(filePath, json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            
            LOG.info("Rules backup saved: file={}, version={}, ruleCount={}, source={}", 
                fileName, version, rules.size(), source);
            
            // 清理旧备份
            cleanOldBackups();
            
            return true;
            
        } catch (Exception e) {
            LOG.error("Failed to save rules backup: version={}", version, e);
            return false;
        }
    }
    
    /**
     * 从备份文件加载规则（加载版本号最大的备份）
     * 
     * @return 规则备份对象，如果没有备份则返回 null
     */
    public static RuleBackup loadLatestBackup() {
        try {
            File backupDir = new File(BACKUP_DIR);
            if (!backupDir.exists() || !backupDir.isDirectory()) {
                LOG.warn("Backup directory does not exist: {}", BACKUP_DIR);
                return null;
            }
            
            // 获取所有备份文件
            File[] backupFiles = backupDir.listFiles((dir, name) -> 
                name.startsWith(BACKUP_FILE_PREFIX) && name.endsWith(BACKUP_FILE_SUFFIX));
            
            if (backupFiles == null || backupFiles.length == 0) {
                LOG.warn("No backup files found in: {}", BACKUP_DIR);
                return null;
            }
            
            // 按版本号排序（而不是时间戳）
            // 这样可以确保加载版本号最大的备份，即使时间戳不是最新的
            File latestFile = null;
            long maxVersion = 0;
            
            for (File file : backupFiles) {
                // 从文件名提取版本号
                String fileName = file.getName();
                try {
                    String[] parts = fileName.replace(BACKUP_FILE_PREFIX, "")
                                             .replace(BACKUP_FILE_SUFFIX, "")
                                             .split("_");
                    if (parts.length >= 2) {
                        long version = Long.parseLong(parts[0]);  // 提取版本号（第一个部分）
                        if (version > maxVersion) {
                            maxVersion = version;
                            latestFile = file;
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Invalid backup file name: {}", fileName);
                }
            }
            
            if (latestFile == null) {
                LOG.warn("No valid backup files found");
                return null;
            }
            
            // 读取备份文件
            byte[] bytes = Files.readAllBytes(latestFile.toPath());
            String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            RuleBackup backup = JSON.parseObject(json, RuleBackup.class);
            
            LOG.info("Loaded backup from file: {}, version={}, ruleCount={}, ageInMinutes={}", 
                latestFile.getName(), backup.getVersion(), backup.getRuleCount(), 
                backup.getAgeInMinutes());
            
            return backup;
            
        } catch (Exception e) {
            LOG.error("Failed to load backup from directory: {}", BACKUP_DIR, e);
            return null;
        }
    }
    
    /**
     * 获取所有备份文件信息
     * 
     * @return 备份文件列表（按时间戳降序）
     */
    public static List<RuleBackup> listAllBackups() {
        List<RuleBackup> backups = new ArrayList<>();
        
        try {
            File backupDir = new File(BACKUP_DIR);
            if (!backupDir.exists() || !backupDir.isDirectory()) {
                return backups;
            }
            
            File[] backupFiles = backupDir.listFiles((dir, name) -> 
                name.startsWith(BACKUP_FILE_PREFIX) && name.endsWith(BACKUP_FILE_SUFFIX));
            
            if (backupFiles == null || backupFiles.length == 0) {
                return backups;
            }
            
            for (File file : backupFiles) {
                try {
                    byte[] bytes = Files.readAllBytes(file.toPath());
                    String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                    RuleBackup backup = JSON.parseObject(json, RuleBackup.class);
                    backups.add(backup);
                } catch (Exception e) {
                    LOG.warn("Failed to read backup file: {}", file.getName(), e);
                }
            }
            
            // 按时间戳降序排序
            backups.sort(Comparator.comparing(RuleBackup::getTimestamp).reversed());
            
        } catch (Exception e) {
            LOG.error("Failed to list backups", e);
        }
        
        return backups;
    }
    
    /**
     * 清理旧备份（保留最近 3 个版本）
     */
    private static void cleanOldBackups() {
        try {
            File backupDir = new File(BACKUP_DIR);
            if (!backupDir.exists() || !backupDir.isDirectory()) {
                return;
            }
            
            File[] backupFiles = backupDir.listFiles((dir, name) -> 
                name.startsWith(BACKUP_FILE_PREFIX) && name.endsWith(BACKUP_FILE_SUFFIX));
            
            if (backupFiles == null || backupFiles.length <= MAX_BACKUP_COUNT) {
                return;
            }
            
            // 按修改时间排序
            List<File> sortedFiles = new ArrayList<>();
            for (File file : backupFiles) {
                sortedFiles.add(file);
            }
            sortedFiles.sort(Comparator.comparing(File::lastModified).reversed());
            
            // 删除超过保留数量的文件
            int deletedCount = 0;
            for (int i = MAX_BACKUP_COUNT; i < sortedFiles.size(); i++) {
                File fileToDelete = sortedFiles.get(i);
                if (fileToDelete.delete()) {
                    deletedCount++;
                    LOG.info("Deleted old backup file: {}", fileToDelete.getName());
                } else {
                    LOG.warn("Failed to delete old backup file: {}", fileToDelete.getName());
                }
            }
            
            if (deletedCount > 0) {
                LOG.info("Cleaned {} old backup files, kept {} latest backups", 
                    deletedCount, MAX_BACKUP_COUNT);
            }
            
        } catch (Exception e) {
            LOG.error("Failed to clean old backups", e);
        }
    }
    
    /**
     * 删除所有备份文件
     */
    public static void deleteAllBackups() {
        try {
            File backupDir = new File(BACKUP_DIR);
            if (!backupDir.exists() || !backupDir.isDirectory()) {
                return;
            }
            
            File[] backupFiles = backupDir.listFiles((dir, name) -> 
                name.startsWith(BACKUP_FILE_PREFIX) && name.endsWith(BACKUP_FILE_SUFFIX));
            
            if (backupFiles == null || backupFiles.length == 0) {
                return;
            }
            
            int deletedCount = 0;
            for (File file : backupFiles) {
                if (file.delete()) {
                    deletedCount++;
                }
            }
            
            LOG.info("Deleted all {} backup files", deletedCount);
            
        } catch (Exception e) {
            LOG.error("Failed to delete all backups", e);
        }
    }
    
    /**
     * 获取备份统计信息
     */
    public static String getBackupStats() {
        try {
            List<RuleBackup> backups = listAllBackups();
            
            if (backups.isEmpty()) {
                return "No backups available";
            }
            
            RuleBackup latest = backups.get(0);
            
            return String.format("Backups: count=%d, latest={version=%d, ageInMinutes=%d, ruleCount=%d}", 
                backups.size(), latest.getVersion(), latest.getAgeInMinutes(), latest.getRuleCount());
            
        } catch (Exception e) {
            return "Failed to get backup stats: " + e.getMessage();
        }
    }
}
