#!/bin/bash

# 候选的exchange list
exchange_list=(
    "binance"
    "okex"
    "bybit"
)

side=""
exchange=""

usage() {
    cat <<USAGE
用法: $(basename "$0") [--side primary|secondary] <exchange>

参数:
  --side        选择网关分区: primary 或 secondary（仅 binance 需要）

示例:
  $(basename "$0") --side primary binance
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --side)
            shift
            if [ $# -eq 0 ]; then
                echo "缺少 --side 参数值"
                usage
                exit 1
            fi
            side="$1"
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        -* )
            echo "未知参数: $1"
            usage
            exit 1
            ;;
        *)
            if [ -z "$exchange" ]; then
                exchange="$1"
            else
                echo "多余参数: $1"
                usage
                exit 1
            fi
            shift
            ;;
    esac
done

if [ -z "$exchange" ]; then
    echo "请提供一个交易所名称作为参数"
    echo "支持的交易所: ${exchange_list[*]}"
    usage
    exit 1
fi

if ! echo "${exchange_list[@]}" | grep -wq "$exchange"; then
    echo "无效的交易所名称: $exchange"
    echo "支持的交易所: ${exchange_list[*]}"
    exit 1
fi

if [ "$exchange" = "binance" ]; then
    if [ -z "$side" ]; then
        echo "请提供 --side 参数"
        usage
        exit 1
    fi

    side="${side,,}"
    case "$side" in
        primary)
            BINANCE_GATEWAY_HOST="192.168.1.198"
            ;;
        secondary)
            BINANCE_GATEWAY_HOST="192.168.1.55"
            ;;
        *)
            echo "无效的 --side 参数: $side"
            usage
            exit 1
            ;;
    esac
else
    if [ -n "$side" ]; then
        side="${side,,}"
        case "$side" in
            primary|secondary)
                ;;
            *)
                echo "无效的 --side 参数: $side"
                usage
                exit 1
                ;;
        esac
    fi
fi

# 根据交易所获取对应的合约和现货配置
get_exchange_configs() {
    local exchange=$1
    case $exchange in
        "binance")
            echo "binance binance-futures"
            ;;
        "okex")
            echo "okex okex-swap"
            ;;
        "bybit")
            echo "bybit bybit-spot"
            ;;
        *)
            echo "未知交易所: $exchange"
            exit 1
            ;;
    esac
}

# 启动单个代理的函数
start_single_proxy() {
    local config=$1
    local pm2_name="crypto_proxy_${config}"

    echo "启动 ${config} 代理..."

    # 停止现有进程
    pm2 delete "$pm2_name" 2>/dev/null

    # 启动新进程
    local rest_args=()
    if [[ "$config" == "binance" || "$config" == "binance-futures" ]]; then
        rest_args+=(--binance-url "http://${BINANCE_GATEWAY_HOST}:19080")
        rest_args+=(--binance-futures-url "http://${BINANCE_GATEWAY_HOST}:19081")
    fi

    pm2 start ./crypto_proxy \
        --name "$pm2_name" \
        -- \
        --exchange "$config" \
        "${rest_args[@]}"

    if [ $? -eq 0 ]; then
        echo "✓ ${config} 代理启动成功"
    else
        echo "✗ ${config} 代理启动失败"
        return 1
    fi
}

# 主启动函数
start_exchange_proxies() {
    local exchange=$1
    local configs=$(get_exchange_configs "$exchange")

    echo "开始启动 ${exchange} 交易所的代理服务..."
    echo "将启动以下配置: $configs"
    if [ "$exchange" = "binance" ]; then
        echo "使用网关: ${BINANCE_GATEWAY_HOST} (side: ${side})"
    fi
    echo ""

    for config in $configs; do
        start_single_proxy "$config"
        sleep 2
    done

    echo ""
    echo "${exchange} 交易所所有代理启动完成！"
    echo ""
    echo "查看日志命令:"
    for config in $configs; do
        echo "  pm2 logs crypto_proxy_${config}"
    done

    echo ""
    echo "查看状态: pm2 status"
}

start_exchange_proxies "$exchange"
