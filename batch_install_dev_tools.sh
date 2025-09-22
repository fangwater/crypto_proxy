#!/bin/bash

# 批量安装开发工具脚本
# 功能：
# 1. 使用el01用户登录
# 2. 添加PPA源
# 3. 安装GCC/G++ 13
# 4. 安装Node.js 22

# 配置变量
USER="el01"

# 服务器IP列表 - 请在这里添加你的服务器IP
SERVER_LIST=(
    "192.168.1.100"
    "192.168.1.101"
    "192.168.1.102"
    # 添加更多服务器IP...
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

# 安装单台服务器的开发工具
install_dev_tools() {
    local server_ip=$1
    
    log_info "开始配置服务器: $server_ip"
    
    # 创建远程执行脚本
    cat > /tmp/install_tools_${server_ip//./}.sh << 'EOF'
#!/bin/bash

# 检测操作系统
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
    VER=$VERSION_ID
else
    echo "无法检测操作系统版本"
    exit 1
fi

echo "检测到操作系统: $OS $VER"

# 更新系统包
echo "更新系统包..."
sudo apt update

# 添加PPA源和安装GCC/G++ 13
echo "添加PPA源并安装GCC/G++ 13..."
sudo apt install -y software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt update

# 安装GCC/G++ 13
sudo apt install -y gcc-13 g++-13

# 设置默认版本
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 60 --slave /usr/bin/g++ g++ /usr/bin/g++-13
echo "GCC/G++ 13 安装完成"

# 安装Node.js 22
echo "安装Node.js 22..."

# 添加NodeSource repository
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -

# 安装Node.js
sudo apt install -y nodejs

echo "Node.js 安装完成"

# 验证安装
echo "========================================="
echo "安装完成，版本信息："
echo "========================================="
gcc --version | head -1
g++ --version | head -1
node --version
npm --version
echo "========================================="
EOF
    
    log_info "[$server_ip] 上传并执行安装脚本..."
    
    # 测试SSH连接
    if ! ssh -o ConnectTimeout=5 "$USER@$server_ip" "echo 'SSH连接正常'" 2>/dev/null; then
        log_error "[$server_ip] 无法连接到服务器"
        return 1
    fi
    
    # 复制脚本到远程服务器并执行
    if scp /tmp/install_tools_${server_ip//./}.sh "$USER@$server_ip:/tmp/install_tools.sh"; then
        if ssh "$USER@$server_ip" "chmod +x /tmp/install_tools.sh && /tmp/install_tools.sh && rm /tmp/install_tools.sh"; then
            log_info "[$server_ip] 开发工具安装成功"
        else
            log_error "[$server_ip] 安装脚本执行失败"
            return 1
        fi
    else
        log_error "[$server_ip] 脚本上传失败"
        return 1
    fi
    
    # 清理临时文件
    rm -f /tmp/install_tools_${server_ip//./}.sh
    
    log_info "[$server_ip] 配置完成\n"
    return 0
}

# 主函数
main() {
    echo "========================================="
    echo "      批量安装开发工具脚本"
    echo "========================================="
    
    # 检查服务器列表
    if [ ${#SERVER_LIST[@]} -eq 0 ]; then
        log_error "服务器列表为空"
        log_info "请在脚本中的 SERVER_LIST 数组中添加服务器IP"
        exit 1
    fi
    
    log_info "使用用户: $USER"
    log_info "将安装: GCC/G++ 13, Node.js 22"
    echo
    
    # 确认继续
    echo -n "是否继续批量安装？(y/N): "
    read -r confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        log_info "操作已取消"
        exit 0
    fi
    
    success_count=0
    fail_count=0
    
    # 遍历服务器列表
    for server_ip in "${SERVER_LIST[@]}"; do
        # 跳过注释行
        [[ "$server_ip" =~ ^[[:space:]]*# ]] && continue
        
        # 安装开发工具
        if install_dev_tools "$server_ip"; then
            ((success_count++))
        else
            ((fail_count++))
        fi
        
        # 服务器间暂停
        sleep 2
    done
    
    # 输出统计结果
    echo "========================================="
    echo "         安装完成统计"
    echo "========================================="
    log_info "成功安装: $success_count 台服务器"
    if [ $fail_count -gt 0 ]; then
        log_error "安装失败: $fail_count 台服务器"
    fi
    echo "========================================="
    
    log_info "可以使用以下命令验证安装:"
    echo "ssh $USER@<服务器IP> 'gcc --version && node --version'"
}

# 运行主函数
main "$@"