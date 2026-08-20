package com.dataplatform.quality;

import com.dataplatform.quality.cache.RuleCacheManager;
import com.dataplatform.quality.rule.RuleLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Properties;

/**
 * Spring Boot 应用启动类 - 用于缓存性能测试
 * 启动命令: java -jar target/data-quality-platform-1.0-SNAPSHOT.jar --spring.profiles.active=test
 */
@SpringBootApplication
public class CacheTestApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(CacheTestApplication.class, args);
    }
    
    /**
     * 初始化 RuleLoader Bean
     */
    @Bean
    public RuleLoader ruleLoader() throws IOException {
        Properties config = new Properties();
        config.load(CacheTestApplication.class.getClassLoader().getResourceAsStream("application.properties"));
        
        return new RuleLoader(
            config.getProperty("mysql.url"),
            config.getProperty("mysql.username"),
            config.getProperty("mysql.password")
        );
    }
    
    /**
     * 初始化 RuleCacheManager Bean
     */
    @Bean
    public RuleCacheManager ruleCacheManager(RuleLoader ruleLoader) throws IOException {
        Properties config = new Properties();
        config.load(CacheTestApplication.class.getClassLoader().getResourceAsStream("application.properties"));
        
        String redisHost = config.getProperty("redis.host", "localhost");
        int redisPort = Integer.parseInt(config.getProperty("redis.port", "6379"));
        
        JedisPool jedisPool = new JedisPool(redisHost, redisPort);
        return new RuleCacheManager(ruleLoader, jedisPool);
    }
}
