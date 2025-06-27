#!/bin/bash

set -e

NODE_EXPORTER_VERSION="1.8.0"

echo "🔧 Baixando Node Exporter v$NODE_EXPORTER_VERSION..."
cd /opt
curl -LO https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
tar -xzf node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64.tar.gz
mv node_exporter-${NODE_EXPORTER_VERSION}.linux-amd64 node_exporter

echo "👤 Criando usuário node_exporter..."
useradd --no-create-home --shell /usr/sbin/nologin node_exporter || true

echo "🚚 Movendo binário pra /usr/local/bin..."
cp /opt/node_exporter/node_exporter /usr/local/bin/
chown node_exporter:node_exporter /usr/local/bin/node_exporter

echo "📝 Criando serviço systemd..."
cat <<EOF > /etc/systemd/system/node_exporter.service
[Unit]
Description=Node Exporter
After=network.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter
Restart=always

[Install]
WantedBy=multi-user.target
EOF

echo "🔄 Habilitando e iniciando Node Exporter..."
systemctl daemon-reload
systemctl enable node_exporter
systemctl start node_exporter

echo "🔥 Liberando porta 9100 no firewall (firewalld)..."
if firewall-cmd --state &>/dev/null; then
  firewall-cmd --permanent --add-port=9100/tcp
  firewall-cmd --reload
fi

echo "✅ Pronto! Node Exporter rodando em http://$(hostname -I | awk '{print $1}'):9100/metrics"
