#!/bin/bash

pm2_name="crypto_proxy_binance-spot"

echo "停止 binance-spot 代理..."

pm2 delete "$pm2_name" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ binance-spot 代理停止成功"
else
    echo "✗ binance-spot 代理停止失败（可能本来就没运行）"
    exit 1
fi
