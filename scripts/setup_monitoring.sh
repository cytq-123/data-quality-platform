#!/bin/bash

# 数据质量监控平台 - 监控系统部署脚本
# 包含：Prometheus + Grafana + Flink Metrics

set -e

FLINK_HOME="/home/hsy/app/flink"
APP_HOME="/home/hsy/app"
PROJECT_HOME="/home/hsy/1"

echo "========================================="
echo "开始部署监控系统"
echo "========================================="

# 1. 配置 Flink Prometheus Reporter
echo ""
echo "[1/6] 配置 Flink Prometheus Reporter..."

# 检查是否已安装 Prometheus Reporter
if [ ! -f "$FLINK_HOME/lib/flink-metrics-prometheus-1.17.1.jar" ]; then
    echo "下载 Flink Prometheus Reporter..."
    wget -O "$FLINK_HOME/lib/flink-metrics-prometheus-1.17.1.jar" \
        https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.17.1/flink-metrics-prometheus-1.17.1.jar
fi

# 配置 Flink
if ! grep -q "metrics.reporter.prom.class" "$FLINK_HOME/conf/flink-conf.yaml"; then
    echo "添加 Prometheus Reporter 配置到 flink-conf.yaml..."
    cat >> "$FLINK_HOME/conf/flink-conf.yaml" << EOF

# Prometheus Reporter 配置
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9249-9259
EOF
else
    echo "Prometheus Reporter 配置已存在，跳过..."
fi

# 2. 安装 Prometheus
echo ""
echo "[2/6] 安装 Prometheus..."

if [ ! -d "$APP_HOME/prometheus" ]; then
    cd $APP_HOME
    wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
    tar -xzf prometheus-2.45.0.linux-amd64.tar.gz
    mv prometheus-2.45.0.linux-amd64 prometheus
    rm prometheus-2.45.0.linux-amd64.tar.gz
    echo "Prometheus 安装完成"
else
    echo "Prometheus 已安装，跳过..."
fi

# 配置 Prometheus
echo "配置 Prometheus..."
cat > "$APP_HOME/prometheus/prometheus.yml" << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

# 加载告警规则
rule_files:
  - "prometheus_rules.yml"

scrape_configs:
  # Flink JobManager
  - job_name: 'flink-jobmanager'
    static_configs:
      - targets: ['localhost:9249']
        labels:
          instance: 'flink-jobmanager'
          env: 'production'
  
  # Flink TaskManagers
  - job_name: 'flink-taskmanager'
    static_configs:
      - targets: ['localhost:9250', 'localhost:9251', 'localhost:9252']
        labels:
          instance: 'flink-taskmanager'
          env: 'production'
EOF

# 复制告警规则配置
if [ -f "$PROJECT_HOME/config/prometheus_rules.yml" ]; then
    cp "$PROJECT_HOME/config/prometheus_rules.yml" "$APP_HOME/prometheus/"
    echo "告警规则配置已复制"
fi

# 3. 安装 Grafana
echo ""
echo "[3/6] 安装 Grafana..."

if ! command -v grafana-server &> /dev/null; then
    echo "添加 Grafana 仓库..."
    sudo tee /etc/yum.repos.d/grafana.repo<<'REPO'
[grafana]
name=grafana
baseurl=https://packages.grafana.com/oss/rpm
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://packages.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
REPO

    echo "安装 Grafana..."
    sudo yum install -y grafana
    echo "Grafana 安装完成"
else
    echo "Grafana 已安装，跳过..."
fi

# 4. 配置防火墙
echo ""
echo "[4/6] 配置防火墙..."

if systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port=9090/tcp  # Prometheus
    sudo firewall-cmd --permanent --add-port=3000/tcp  # Grafana
    sudo firewall-cmd --permanent --add-port=9249-9259/tcp  # Flink Metrics
    sudo firewall-cmd --reload
    echo "防火墙规则已更新"
else
    echo "防火墙未启用，跳过..."
fi

# 5. 启动服务
echo ""
echo "[5/6] 启动服务..."

# 重启 Flink (应用新的 Metrics 配置)
echo "重启 Flink 集群..."
$FLINK_HOME/bin/stop-cluster.sh || true
sleep 3
$FLINK_HOME/bin/start-cluster.sh

# 启动 Prometheus
echo "启动 Prometheus..."
cd $APP_HOME/prometheus
pkill -f prometheus || true
nohup ./prometheus --config.file=prometheus.yml > prometheus.log 2>&1 &
echo "Prometheus 启动完成 (端口: 9090)"

# 启动 Grafana
echo "启动 Grafana..."
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
echo "Grafana 启动完成 (端口: 3000)"

# 6. 验证服务
echo ""
echo "[6/6] 验证服务状态..."

sleep 5

echo "检查 Flink Metrics 端点..."
if curl -s http://localhost:9249/metrics > /dev/null; then
    echo "✓ Flink Metrics 端点正常 (http://localhost:9249/metrics)"
else
    echo "✗ Flink Metrics 端点异常"
fi

echo "检查 Prometheus..."
if curl -s http://localhost:9090/-/healthy > /dev/null; then
    echo "✓ Prometheus 正常 (http://localhost:9090)"
else
    echo "✗ Prometheus 异常"
fi

echo "检查 Grafana..."
if curl -s http://localhost:3000/api/health > /dev/null; then
    echo "✓ Grafana 正常 (http://localhost:3000)"
else
    echo "✗ Grafana 异常"
fi

# 7. 输出访问信息
echo ""
echo "========================================="
echo "监控系统部署完成！"
echo "========================================="
echo ""
echo "访问地址："
echo "  - Flink Web UI:   http://192.168.128.141:8081"
echo "  - Prometheus:     http://192.168.128.141:9090"
echo "  - Grafana:        http://192.168.128.141:3000"
echo ""
echo "Grafana 默认登录："
echo "  用户名: admin"
echo "  密码:   admin (首次登录需修改)"
echo ""
echo "下一步操作："
echo "  1. 访问 Grafana: http://192.168.128.141:3000"
echo "  2. 添加 Prometheus 数据源: http://localhost:9090"
echo "  3. 导入 Dashboard 配置:"
echo "     - $PROJECT_HOME/config/grafana_data_quality_dashboard.json"
echo "     - $PROJECT_HOME/config/grafana_rule_loading_dashboard.json"
echo ""
echo "查看日志："
echo "  - Prometheus: tail -f $APP_HOME/prometheus/prometheus.log"
echo "  - Grafana:    sudo journalctl -u grafana-server -f"
echo "  - Flink:      tail -f $FLINK_HOME/log/flink-*.log"
echo ""
