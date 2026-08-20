package com.dataplatform.quality.sink;

import com.dataplatform.quality.model.QualityMetrics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;

/**
 * ClickHouse Sink - 写入质量指标到 ClickHouse
 */
public class ClickHouseSink extends RichSinkFunction<QualityMetrics> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSink.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient Connection connection;
    private transient Statement statement;
    
    public ClickHouseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 建立 ClickHouse 连接
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        statement = connection.createStatement();
        
        LOG.info("ClickHouseSink initialized, url: {}", jdbcUrl);
    }
    
    @Override
    public void invoke(QualityMetrics metrics, Context context) throws Exception {
        try {
            String ruleName = metrics.getRuleName() != null ? metrics.getRuleName() : "ALL";
            long checkTime = metrics.getCheckTime() / 1000; // 转换为秒
            
            String sql = String.format(
                "INSERT INTO data_quality_metrics " +
                "(rule_name, check_time, total_count, valid_count, invalid_count, pass_rate) " +
                "VALUES ('%s', %d, %d, %d, %d, %.4f)",
                ruleName.replace("'", "''"), // 转义单引号
                checkTime,
                metrics.getTotalCount(),
                metrics.getValidCount(),
                metrics.getInvalidCount(),
                metrics.getPassRate()
            );
            
            statement.execute(sql);
            
            LOG.debug("Quality metrics inserted: ruleName={}, passRate={:.2f}%", 
                ruleName, metrics.getPassRate() * 100);
                
        } catch (SQLException e) {
            LOG.error("Error inserting quality metrics to ClickHouse", e);
            // 不抛出异常,避免影响 Flink 任务
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        if (statement != null) {
            statement.close();
        }
        
        if (connection != null) {
            connection.close();
        }
        
        LOG.info("ClickHouseSink closed");
    }
}
