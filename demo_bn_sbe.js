const WebSocket = require('ws');
const fs = require('fs');

// Binance SBE WebSocket 地址
const sbeUrl = 'wss://stream-sbe.binance.com:9443/ws';

// 创建输出文件流
const logFile = fs.createWriteStream('sbe_test.log', { flags: 'w' });

// 创建 WebSocket 连接
const ws = new WebSocket(sbeUrl, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

// 统计变量
let pushCount = 0;
let lastPushTime = Date.now();
let messageTypes = new Set();
let totalBytesReceived = 0;

ws.on('open', () => {
  console.log('🚀 Binance SBE WebSocket 连接已建立');
  console.log('📡 连接地址:', sbeUrl);
  console.log('⏰ 开始时间:', new Date().toISOString());
  console.log('================================================\n');
  
  logFile.write(`Connection established at ${new Date().toISOString()}\n`);
  
  // 尝试订阅一些常见的流，参考标准币安流格式
  const subscriptions = [
    "btcusdt@ticker",
    "btcusdt@depth5@100ms", 
    "btcusdt@bookTicker",
    "btcusdt@trade"
  ];
  
  subscriptions.forEach((stream, index) => {
    setTimeout(() => {
      const subscribeMsg = {
        method: "SUBSCRIBE",
        params: [stream],
        id: index + 1
      };
      
      console.log(`📤 尝试订阅 ${stream}...`);
      logFile.write(`Subscribing to: ${stream}\n`);
      ws.send(JSON.stringify(subscribeMsg));
    }, index * 1000); // 每秒发送一个订阅
  });
});

ws.on('message', (data) => {
  const currentTime = Date.now();
  const timeDiff = currentTime - lastPushTime;
  lastPushTime = currentTime;
  pushCount++;
  
  // 统计数据大小
  const dataSize = data.length;
  totalBytesReceived += dataSize;
  
  // 记录到文件
  logFile.write(`${currentTime},${pushCount},${dataSize},${timeDiff}\n`);
  
  console.log(`\n📨 消息 #${pushCount} (间隔: ${timeDiff}ms, 大小: ${dataSize} bytes)`);
  
  // 尝试解析为文本
  const dataStr = data.toString();
  
  try {
    // 尝试解析为JSON
    const message = JSON.parse(dataStr);
    console.log(`📋 JSON消息类型: ${typeof message}`);
    
    if (message.result !== undefined) {
      console.log(`✅ 订阅响应:`, message);
    } else if (message.stream) {
      console.log(`📊 数据流: ${message.stream}`);
      messageTypes.add(message.stream);
      
      // 显示数据内容概要
      if (message.data) {
        const dataKeys = Object.keys(message.data);
        console.log(`   数据字段: ${dataKeys.join(', ')}`);
        
        // 如果是ticker数据，显示一些关键信息
        if (message.data.s && message.data.c) {
          console.log(`   🏷️  Symbol: ${message.data.s}, 价格: ${message.data.c}`);
        }
        
        // 如果是depth数据
        if (message.data.bids || message.data.asks) {
          console.log(`   📈 Depth数据 - Bids: ${message.data.bids?.length || 0}, Asks: ${message.data.asks?.length || 0}`);
        }
        
        // 如果是trade数据
        if (message.data.p && message.data.q) {
          console.log(`   💰 Trade - 价格: ${message.data.p}, 数量: ${message.data.q}`);
        }
      }
    } else if (Array.isArray(message)) {
      console.log(`📦 数组消息，长度: ${message.length}`);
    } else {
      console.log(`❓ 其他JSON消息:`, JSON.stringify(message).substring(0, 200));
    }
    
  } catch (e) {
    // 不是JSON，可能是二进制SBE格式
    console.log(`🔧 非JSON数据 (可能是SBE二进制格式)`);
    console.log(`   前32字节 (hex): ${data.slice(0, 32).toString('hex')}`);
    console.log(`   前32字节 (ascii): ${data.slice(0, 32).toString('ascii').replace(/[^\x20-\x7E]/g, '.')}`);
    
    // 记录到文件
    logFile.write(`Binary data: ${data.toString('hex')}\n`);
    messageTypes.add('BINARY_SBE');
  }
  
  // 每10次消息显示统计
  if (pushCount % 10 === 0) {
    console.log('\n📊 统计汇总:');
    console.log(`- 累计消息数: ${pushCount}`);
    console.log(`- 累计流量: ${(totalBytesReceived / 1024).toFixed(2)} KB`);
    console.log(`- 平均消息大小: ${(totalBytesReceived / pushCount).toFixed(0)} bytes`);
    console.log(`- 消息类型: ${Array.from(messageTypes).join(', ')}`);
    console.log(`- 最近消息间隔: ${timeDiff}ms`);
    console.log('================================================');
  }
});

ws.on('error', (err) => {
  console.error('❌ WebSocket 错误:', err);
  logFile.write(`Error: ${err.message}\n`);
});

ws.on('close', (code, reason) => {
  const closeTime = new Date().toISOString();
  console.log('\n🔌 WebSocket 连接已关闭');
  console.log(`📅 关闭时间: ${closeTime}`);
  console.log(`🔢 关闭代码: ${code}`);
  console.log(`📝 关闭原因: ${reason || 'N/A'}`);
  
  console.log('\n📈 最终统计:');
  console.log(`- 总消息数: ${pushCount}`);
  console.log(`- 总流量: ${(totalBytesReceived / 1024).toFixed(2)} KB`);
  console.log(`- 平均消息大小: ${pushCount > 0 ? (totalBytesReceived / pushCount).toFixed(0) : 0} bytes`);
  console.log(`- 发现的消息类型: ${Array.from(messageTypes).join(', ') || '无'}`);
  
  logFile.write(`Connection closed at ${closeTime}, code: ${code}, reason: ${reason}\n`);
  logFile.end();
  
  console.log('\n📁 测试日志已保存到: sbe_test.log');
});

// 程序退出处理
process.on('SIGINT', () => {
  console.log('\n\n⏹️  用户中断，正在关闭连接...');
  ws.close();
});

// 连接超时处理
setTimeout(() => {
  if (ws.readyState === WebSocket.CONNECTING) {
    console.log('⏱️  连接超时，正在中断...');
    ws.terminate();
  }
}, 30000); // 30秒超时