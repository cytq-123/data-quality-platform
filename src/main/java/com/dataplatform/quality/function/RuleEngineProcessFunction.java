# Redis 崩溃检测完全指南

## 一、Redis "崩溃" 的定义

Redis 崩溃不是指进程挂掉，而是指**性能崩溃**，具体表现为：

### 1.1 关键指标异常

| 指标                | 正常值      | 告警值       | 崩溃值        |
|--------------------|-----------|-------------|--------------|
| **响应延迟**         | < 5ms     | > 50ms      | > 500ms      |
| **QPS**            | < 1000    | 1000-1500   | > 2000       |
| **连接数**          | < 100     | 100-500     | > 1000       |
| **内存使用**        | < 50%     | 50%-80%     | > 90%        |
| **CPU 使用率**      | < 30%     | 30%-80%     | > 90%        |
| **慢查询数量**      | 0         | 1-10/分钟   | > 100/分钟   |
| **拒绝连接错误**     | 0         | 偶发        | 频繁         |

### 1.2 业务层表现

- **端到端延迟飙升**：从 210ms 突然跳到 1000ms+
- **Flink 反压**：Kafka Lag 持续增长
- **日志错误**：大量 Redis 超时异常

---

## 二、实时监控 Redis 状态的 4 种方法

### 方法 1：Redis-CLI 实时监控（最直接）

```bash
# SSH 到 Redis 服务器
ssh root@192.168.128.141

# 实时监控 Redis 状态（每秒刷新）
watch -n 1 'redis-cli info stats | grep -E "instantaneous_ops_per_sec|total_commands_processed|used_memory_human|connected_clients|rejected_connections|evicted_keys"'
```

**输出示例**：
```
# 正常状态
instantaneous_ops_per_sec:95          # 实时 QPS：95
connected_clients:12                   # 连接数：12
used_memory_human:125.32M             # 内存：125 MB
rejected_connections:0                 # 拒绝连接：0

# 崩溃状态 ❌
instantaneous_ops_per_sec:2150        # 实时 QPS：2150（超限）
connected_clients:1024                 # 连接数：1024（爆满）
used_memory_human:7.89G               # 内存：7.89 GB（接近上限）
rejected_connections:325               # 拒绝连接：325（频繁拒绝）
```

---

### 方法 2：Redis 慢查询监控

```bash
# 查看慢查询配置
redis-cli CONFIG GET slowlog-*

# 设置慢查询阈值（10ms）
redis-cli CONFIG SET slowlog-log-slower-than 10000

# 实时查看慢查询（每秒刷新）
watch -n 1 'redis-cli SLOWLOG GET 10'
```

**崩溃信号**：慢查询数量突然暴增
```
# 正常状态
slowlog len: 2

# 崩溃状态 ❌
slowlog len: 156  # 短时间内产生大量慢查询
```

---

### 方法 3：Grafana Dashboard 可视化监控（推荐）

你项目中已经有 Grafana，需要添加 Redis 监控面板：

#### 3.1 在 Prometheus 中配置 Redis Exporter

**在 Redis 服务器上安装 Redis Exporter**：
```bash
# 下载 Redis Exporter
wget https://github.com/oliver006/redis_exporter/releases/download/v1.55.0/redis_exporter-v1.55.0.linux-amd64.tar.gz
tar -xzf redis_exporter-v1.55.0.linux-amd64.tar.gz
cd redis_exporter-v1.55.0.linux-amd64

# 启动 Redis Exporter
nohup ./redis_exporter --redis.addr=192.168.128.141:6379 &
```

**修改 `config/prometheus.yml`，添加 Redis 监控**：
```yaml
scrape_configs:
  - job_name: 'redis'
    static_configs:
      - targets: ['192.168.128.141:9121']  # Redis Exporter 默认端口
```

**重启 Prometheus**：
```bash
systemctl restart prometheus
```

#### 3.2 Grafana 添加 Redis 面板

在 `config/grafana_redis_dashboard.json` 中添加：

**核心监控指标**：
1. **Redis QPS** (`redis_instantaneous_ops_per_sec`)
2. **Redis 响应延迟** (`redis_slowlog_length`)
3. **连接数** (`redis_connected_clients`)
4. **内存使用** (`redis_memory_used_bytes`)
5. **拒绝连接数** (`redis_rejected_connections_total`)

---

### 方法 4：应用层异常日志监控（最准确）

Redis 崩溃时，Flink 应用会抛出异常。我们在代码中添加监控：

#### 4.1 在 RuleEngineProcessFunction 中添加 Redis 异常计数

<function_calls>
<invoke name="read_file">
<parameter name="explanation">读取RuleEngineProcessFunction查看Redis调用位置