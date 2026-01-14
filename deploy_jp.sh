#!/bin/bash

#部署脚本
#cfg在项目根目录的mkt_cfg.yaml
# 给定多台机器的ip，并指定primary_ip和secondary_ip

# 设置错误时立即退出
set -e

# 设置SSH超时时间
SSH_TIMEOUT=10

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 检查命令执行状态
check_status() {
    if [ $? -eq 0 ]; then
        log "✅ $1 成功"
    else
        log "❌ $1 失败"
        exit 1
    fi
}

# 部署模式: full 或 bin_only
DEPLOY_MODE=${DEPLOY_MODE:-full}

# 服务器配置
# 格式: "IP:角色" (角色: primary 或 secondary)
SERVERS=(
    #  "103.90.136.194:primary"
    # "103.90.136.195:secondary"
    # "156.231.137.90:primary"
    # "156.231.137.91:secondary"
    # "103.90.136.196:primary"
    # "103.90.136.197:secondary"
      "156.231.137.92:primary",
      "156.231.137.188:secondary"
)

user=el01
exec_dir=/home/$user/crypto_mkt

# 解析服务器配置的函数
parse_server_config() {
    local config="$1"
    local ip="${config%:*}"
    local role="${config#*:}"
    echo "$ip $role"
}

# 检查所有服务器的SSH连接
log "检查所有服务器的SSH连接..."
for server_config in "${SERVERS[@]}"; do
    # 跳过注释行
    [[ "$server_config" =~ ^[[:space:]]*# ]] && continue
    
    read -r ip role <<< $(parse_server_config "$server_config")
    
    log "检查服务器 $ip ($role)..."
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "echo 'SSH连接成功'" > /dev/null 2>&1
    check_status "SSH连接到 $ip"
    
    # 检查exec_dir目录是否存在
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "if [ ! -d $exec_dir ]; then sudo mkdir -p $exec_dir && sudo chown $user:$user $exec_dir; fi"
    check_status "检查目录在 $ip"
done

# 编译项目
log "开始编译项目..."
cargo build --release -j2
check_status "项目编译"

# 记录部署模式
if [ "$DEPLOY_MODE" = "bin_only" ]; then
    log "当前部署模式: 仅更新二进制，跳过脚本同步"
else
    log "当前部署模式: 全量部署（二进制 + 脚本）"
fi

# 部署二进制文件和脚本
log "开始部署..."

for server_config in "${SERVERS[@]}"; do
    # 跳过注释行
    [[ "$server_config" =~ ^[[:space:]]*# ]] && continue
    
    read -r ip role <<< $(parse_server_config "$server_config")
    
    log "部署到服务器 $ip ($role)..."
    
    # 部署二进制文件
    scp -o ConnectTimeout=$SSH_TIMEOUT target/release/crypto_proxy $user@$ip:$exec_dir/crypto_proxy
    check_status "复制二进制文件到 $ip"
    
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chmod +x $exec_dir/crypto_proxy"
    check_status "设置 $ip 上的二进制文件权限"
    
    if [ "$DEPLOY_MODE" != "bin_only" ]; then
        # 部署脚本文件
        scp -o ConnectTimeout=$SSH_TIMEOUT start_proxy.sh stop_proxy.sh $user@$ip:$exec_dir/
        check_status "复制脚本文件到 $ip"
        
        ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chmod +x $exec_dir/start_proxy.sh $exec_dir/stop_proxy.sh"
        check_status "设置 $ip 上的脚本文件权限"
    else
        log "跳过服务器 $ip 的启动/停止脚本同步"
    fi
    
    log "服务器 $ip ($role) 部署完成！"
done

log "所有服务器部署完成！"




