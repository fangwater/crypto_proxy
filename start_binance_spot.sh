#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/env.sh"

if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
else
    echo "未找到 env.sh: $ENV_FILE"
    exit 1
fi

side=""

usage() {
    cat <<USAGE
用法: $(basename "$0") --side primary|secondary

示例:
  $(basename "$0") --side primary
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
            echo "多余参数: $1"
            usage
            exit 1
            ;;
    esac
done

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

pm2_name="crypto_proxy_binance-spot"
config="binance-spot"

echo "启动 ${config} 代理..."

pm2 delete "$pm2_name" 2>/dev/null

pm2 start ./crypto_proxy \
    --name "$pm2_name" \
    -- \
    --exchange "$config" \
    --binance-url "http://${BINANCE_GATEWAY_HOST}:19080" \
    --binance-futures-url "http://${BINANCE_GATEWAY_HOST}:19081"

if [ $? -eq 0 ]; then
    echo "✓ ${config} 代理启动成功"
else
    echo "✗ ${config} 代理启动失败"
    exit 1
fi

echo "查看日志: pm2 logs ${pm2_name}"
