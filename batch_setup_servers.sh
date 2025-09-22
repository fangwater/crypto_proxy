#!/bin/bash

# 批量配置服务器脚本
# 功能：
# 1. 配置SSH免密登录
# 2. 创建el01用户
# 3. 配置sudo权限
# 4. 设置密码

set -e  # 遇到错误立即退出

# 配置变量
ROOT_PASSWORD=""  # 将在运行时提示输入
NEW_USER="el01"
NEW_USER_PASSWORD="elfhzdev"

# 服务器IP列表 - 请在这里添加你的服务器IP
# 格式：("IP" "IP:用户名")
SERVER_LIST=(
    "163.227.14.73"
    "163.227.14.74"
    "163.227.14.75" 
    "163.227.14.76"
    "163.227.14.77"
)

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要工具
check_requirements() {
    log_info "检查必要工具..."
    
    if ! command -v sshpass &> /dev/null; then
        log_error "sshpass 未安装，正在安装..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install sshpass
        else
            sudo apt-get install -y sshpass || sudo yum install -y sshpass
        fi
    fi
    
    if ! command -v ssh-copy-id &> /dev/null; then
        log_error "ssh-copy-id 未找到"
        exit 1
    fi
    
    log_info "工具检查完成"
}

# 配置单台服务器
setup_server() {
    local server_ip="$1"
    local root_user="${2:-root}"
    
    log_info "开始配置服务器: $server_ip"
    
    # 1. 配置SSH免密登录
    log_info "[$server_ip] 配置SSH免密登录..."
    if sshpass -p "$ROOT_PASSWORD" ssh-copy-id -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$root_user@$server_ip" 2>/dev/null; then
        log_info "[$server_ip] SSH公钥复制成功"
    else
        log_error "[$server_ip] SSH公钥复制失败"
        return 1
    fi
    
    # 2. 创建用户和配置
    log_info "[$server_ip] 创建用户和配置权限..."
    
    # 创建远程执行脚本
    local temp_script="/tmp/setup_user_$(echo "$server_ip" | tr '.' '_').sh"
    cat > "$temp_script" << EOF
#!/bin/bash

# 创建用户
if id "$NEW_USER" &>/dev/null; then
    echo "用户 $NEW_USER 已存在"
else
    useradd -m -s /bin/bash "$NEW_USER"
    echo "用户 $NEW_USER 创建成功"
fi

# 设置密码
echo "$NEW_USER:$NEW_USER_PASSWORD" | chpasswd
echo "用户 $NEW_USER 密码设置成功"

# 配置sudo权限
if ! grep -q "$NEW_USER" /etc/sudoers; then
    echo "$NEW_USER ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
    echo "用户 $NEW_USER sudo权限配置成功"
else
    echo "用户 $NEW_USER sudo权限已存在"
fi

# 创建.ssh目录并设置权限
sudo -u "$NEW_USER" mkdir -p /home/$NEW_USER/.ssh
sudo -u "$NEW_USER" chmod 700 /home/$NEW_USER/.ssh

# 复制当前用户的authorized_keys到新用户
if [ -f /root/.ssh/authorized_keys ]; then
    cp /root/.ssh/authorized_keys /home/$NEW_USER/.ssh/
    chown $NEW_USER:$NEW_USER /home/$NEW_USER/.ssh/authorized_keys
    chmod 600 /home/$NEW_USER/.ssh/authorized_keys
    echo "SSH密钥复制到新用户成功"
fi

echo "服务器配置完成"
EOF
    
    # 复制脚本到远程服务器并执行
    if scp -o ConnectTimeout=10 "$temp_script" "$root_user@$server_ip:/tmp/setup_user.sh"; then
        if ssh -o ConnectTimeout=10 "$root_user@$server_ip" "chmod +x /tmp/setup_user.sh && /tmp/setup_user.sh && rm /tmp/setup_user.sh"; then
            log_info "[$server_ip] 服务器配置成功"
            
            # 测试新用户SSH连接
            if ssh -o ConnectTimeout=5 "$NEW_USER@$server_ip" "echo 'SSH连接测试成功'" 2>/dev/null; then
                log_info "[$server_ip] 新用户SSH连接测试成功"
            else
                log_warn "[$server_ip] 新用户SSH连接测试失败"
            fi
        else
            log_error "[$server_ip] 远程脚本执行失败"
            return 1
        fi
    else
        log_error "[$server_ip] 脚本上传失败"
        return 1
    fi
    
    # 清理临时文件
    rm -f "$temp_script"
    
    log_info "[$server_ip] 配置完成"
    echo ""
    return 0
}

# 主函数
main() {
    echo "========================================="
    echo "        批量服务器配置脚本"
    echo "========================================="
    
    # 检查服务器列表
    if [ ${#SERVER_LIST[@]} -eq 0 ]; then
        log_error "服务器列表为空"
        log_info "请在脚本中的 SERVER_LIST 数组中添加服务器IP"
        exit 1
    fi
    
    # 检查必要工具
    check_requirements
    
    # 输入root密码
    echo -n "请输入root密码: "
    read -s ROOT_PASSWORD
    echo
    
    if [ -z "$ROOT_PASSWORD" ]; then
        log_error "密码不能为空"
        exit 1
    fi
    
    # 读取服务器列表
    log_info "开始批量配置服务器..."
    log_info "新用户名: $NEW_USER"
    log_info "新用户密码: $NEW_USER_PASSWORD"
    log_info "服务器数量: ${#SERVER_LIST[@]}"
    echo
    
    success_count=0
    fail_count=0
    
    # 遍历服务器列表
    for server_entry in "${SERVER_LIST[@]}"; do
        # 跳过注释行
        [[ "$server_entry" =~ ^[[:space:]]*# ]] && continue
        
        # 解析IP和用户名
        if [[ "$server_entry" == *":"* ]]; then
            server_ip="${server_entry%:*}"
            root_user="${server_entry#*:}"
        else
            server_ip="$server_entry"
            root_user="root"
        fi
        
        # 配置服务器
        if setup_server "$server_ip" "$root_user"; then
            ((success_count++))
        else
            ((fail_count++))
        fi
        
        # 服务器间暂停
        sleep 1
    done
    
    # 输出统计结果
    echo "========================================="
    echo "           配置完成统计"
    echo "========================================="
    log_info "成功配置: $success_count 台服务器"
    if [ $fail_count -gt 0 ]; then
        log_error "配置失败: $fail_count 台服务器"
    fi
    echo "========================================="
    
    log_info "可以使用以下命令测试连接:"
    echo "ssh $NEW_USER@<服务器IP>"
}

# 运行主函数
main "$@"