#!/bin/bash

# 仅部署二进制的快捷脚本
# 复用 deploy_jp.sh 的逻辑，通过 DEPLOY_MODE 控制行为

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_MODE=bin_only bash "$SCRIPT_DIR/deploy_jp.sh" "$@"
