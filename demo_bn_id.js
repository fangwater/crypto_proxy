const WebSocket = require('ws');
const fs = require('fs');

// 创建输出文件流
const logFile = fs.createWriteStream('updateid_timing.log', { flags: 'w' });
const csvFile = fs.createWriteStream('updateid_data.csv', { flags: 'w' });

// 写入CSV头部
csvFile.write('timestamp,updateId,source,sequenceNumber\n');

// Binance 现货 WebSocket 地址
// const wsUrl_spot = 'wss://fstream.binance.com/ws';
const wsUrl_spot = 'wss://data-stream.binance.vision/ws';

// 创建两个 WebSocket 连接
const depthWs = new WebSocket(wsUrl_spot, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

const bookTickerWs = new WebSocket(wsUrl_spot, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
  }
});

// 统计变量
let depthPushCount = 0;
let bookTickerPushCount = 0;
let depthUpdateIds = [];
let bookTickerUpdateIds = [];
let matchedUpdateIds = [];

// 时序分析变量
let timingAnalysis = [];
let depthFirstCount = 0;
let bookTickerFirstCount = 0;
let alternatingPattern = [];

// 时序分析核心函数
function analyzeTimingOrder(updateId, timestamp, streamType) {
  // 查找是否已经有这个updateId的记录
  let existingRecord = timingAnalysis.find(record => record.updateId === updateId);
  
  if (!existingRecord) {
    // 第一次出现这个updateId，创建新记录
    timingAnalysis.push({
      updateId: updateId,
      firstStream: streamType,
      firstTime: timestamp,
      secondStream: null,
      secondTime: null,
      timeDiff: null
    });
  } else if (existingRecord.secondStream === null) {
    // 第二次出现，完成时序记录
    existingRecord.secondStream = streamType;
    existingRecord.secondTime = timestamp;
    existingRecord.timeDiff = timestamp - existingRecord.firstTime;
    
    // 统计哪个流更快
    if (existingRecord.firstStream === 'depth5') {
      depthFirstCount++;
      alternatingPattern.push('D'); // D表示depth先到
      console.log(`⏱️  时序: Depth5 先于 BookTicker ${existingRecord.timeDiff}ms (updateId: ${updateId})`);
    } else {
      bookTickerFirstCount++;
      alternatingPattern.push('B'); // B表示bookTicker先到
      console.log(`⏱️  时序: BookTicker 先于 Depth5 ${existingRecord.timeDiff}ms (updateId: ${updateId})`);
    }
    
    // 保持alternatingPattern不超过50个记录
    if (alternatingPattern.length > 50) {
      alternatingPattern.shift();
    }
    
    // 保持timingAnalysis不超过100个完整记录
    const completeRecords = timingAnalysis.filter(r => r.secondStream !== null);
    if (completeRecords.length > 100) {
      // 移除最老的完整记录
      const oldestCompleteIndex = timingAnalysis.findIndex(r => r.secondStream !== null);
      if (oldestCompleteIndex !== -1) {
        timingAnalysis.splice(oldestCompleteIndex, 1);
      }
    }
  }
}

// Depth5 WebSocket
depthWs.on('open', () => {
  console.log('Depth5 WebSocket 连接已建立');
  
  const subscribeMsg = {
    method: "SUBSCRIBE",
    params: ["btcusdt@depth5@100ms"],
    id: 1
  };

  depthWs.send(JSON.stringify(subscribeMsg));
  console.log('已订阅 btcusdt@depth5@100ms');
});

depthWs.on('message', (data) => {
  try {
    const message = JSON.parse(data.toString());
    
    if (message.lastUpdateId) {
      depthPushCount++;
      const updateId = message.lastUpdateId;
      const timestamp = Date.now();
      
      depthUpdateIds.push({
        updateId: updateId,
        timestamp: timestamp,
        type: 'depth5'
      });
      
      console.log(`📊 Depth5 推送 #${depthPushCount}: updateId=${updateId}`);
      
      // 写入文件
      logFile.write(`${timestamp},${updateId},depth5,${depthPushCount}\n`);
      csvFile.write(`${timestamp},${updateId},depth5,${depthPushCount}\n`);
      
      // 时序分析
      analyzeTimingOrder(updateId, timestamp, 'depth5');
      
      // 检查是否与bookTicker有匹配
      const matched = bookTickerUpdateIds.find(item => item.updateId === updateId);
      if (matched && !matchedUpdateIds.includes(updateId)) {
        matchedUpdateIds.push(updateId);
        console.log(`🎯 匹配发现! UpdateId ${updateId} 在两个流中都出现`);
      }
      
      // 保留最近100个记录
      if (depthUpdateIds.length > 100) {
        depthUpdateIds.shift();
      }
    } else {
      console.log('Depth5 确认消息:', message);
    }
  } catch (e) {
    console.log('Depth5 解析错误:', e.message);
  }
});

// BookTicker WebSocket
bookTickerWs.on('open', () => {
  console.log('BookTicker WebSocket 连接已建立');
  
  const subscribeMsg = {
    method: "SUBSCRIBE", 
    params: ["btcusdt@bookTicker"],
    id: 2
  };

  bookTickerWs.send(JSON.stringify(subscribeMsg));
  console.log('已订阅 btcusdt@bookTicker');
  console.log('================================================\n');
});

bookTickerWs.on('message', (data) => {
  try {
    const message = JSON.parse(data.toString());
    
    if (message.u) {
      bookTickerPushCount++;
      const updateId = message.u;
      const timestamp = Date.now();
      
      bookTickerUpdateIds.push({
        updateId: updateId,
        timestamp: timestamp,
        type: 'bookTicker'
      });
      
      console.log(`⚡ BookTicker 推送 #${bookTickerPushCount}: updateId=${updateId}, 价格=${message.b}|${message.a}`);
      
      // 写入文件
      logFile.write(`${timestamp},${updateId},bookTicker,${bookTickerPushCount}\n`);
      csvFile.write(`${timestamp},${updateId},bookTicker,${bookTickerPushCount}\n`);
      
      // 时序分析
      analyzeTimingOrder(updateId, timestamp, 'bookTicker');
      
      // 检查是否与depth有匹配
      const matched = depthUpdateIds.find(item => item.updateId === updateId);
      if (matched && !matchedUpdateIds.includes(updateId)) {
        matchedUpdateIds.push(updateId);
        console.log(`🎯 匹配发现! UpdateId ${updateId} 在两个流中都出现`);
      }
      
      // 保留最近100个记录
      if (bookTickerUpdateIds.length > 100) {
        bookTickerUpdateIds.shift();
      }
    } else {
      console.log('BookTicker 确认消息:', message);
    }
  } catch (e) {
    console.log('BookTicker 解析错误:', e.message);
  }
});

// 定期统计分析
setInterval(() => {
  if (depthPushCount > 0 && bookTickerPushCount > 0) {
    console.log('\n=== 时序分析报告 ===');
    
    // 基本统计
    console.log(`📊 推送统计: Depth5=${depthPushCount}, BookTicker=${bookTickerPushCount}`);
    console.log(`🎯 匹配的 updateId 数量: ${matchedUpdateIds.length}`);
    
    // 时序分析统计
    const totalTimedPairs = depthFirstCount + bookTickerFirstCount;
    if (totalTimedPairs > 0) {
      console.log('\n--- 速度分析 ---');
      console.log(`⚡ BookTicker 更快: ${bookTickerFirstCount}次 (${(bookTickerFirstCount/totalTimedPairs*100).toFixed(1)}%)`);
      console.log(`📊 Depth5 更快: ${depthFirstCount}次 (${(depthFirstCount/totalTimedPairs*100).toFixed(1)}%)`);
      
      // 计算平均时间差
      const completeTimings = timingAnalysis.filter(t => t.timeDiff !== null);
      if (completeTimings.length > 0) {
        const avgTimeDiff = completeTimings.reduce((sum, t) => sum + Math.abs(t.timeDiff), 0) / completeTimings.length;
        console.log(`⏱️  平均时间差: ${avgTimeDiff.toFixed(2)}ms`);
        
        // 最近的时间差
        const recentTimings = completeTimings.slice(-5);
        const recentDiffs = recentTimings.map(t => `${t.timeDiff}ms`).join(', ');
        console.log(`📈 最近5次时间差: ${recentDiffs}`);
      }
      
      // 交替模式分析
      if (alternatingPattern.length >= 10) {
        console.log('\n--- 模式分析 ---');
        const patternStr = alternatingPattern.slice(-20).join('');
        console.log(`🔄 最近20次模式: ${patternStr}`);
        
        // 检测是否有明显的交替模式
        let consecutiveD = 0, consecutiveB = 0, maxConsecutiveD = 0, maxConsecutiveB = 0;
        for (let pattern of alternatingPattern.slice(-30)) {
          if (pattern === 'D') {
            consecutiveD++;
            consecutiveB = 0;
            maxConsecutiveD = Math.max(maxConsecutiveD, consecutiveD);
          } else {
            consecutiveB++;
            consecutiveD = 0;
            maxConsecutiveB = Math.max(maxConsecutiveB, consecutiveB);
          }
        }
        
        console.log(`📊 最大连续: Depth5=${maxConsecutiveD}, BookTicker=${maxConsecutiveB}`);
        
        // 判断模式类型
        if (maxConsecutiveD >= 5 || maxConsecutiveB >= 5) {
          const winner = maxConsecutiveD > maxConsecutiveB ? 'Depth5' : 'BookTicker';
          console.log(`🏆 结论: ${winner} 具有明显的连续优势`);
        } else if (Math.abs(depthFirstCount - bookTickerFirstCount) <= totalTimedPairs * 0.2) {
          console.log(`🔄 结论: 两个流交替领先，无明显固定模式`);
        } else {
          const winner = depthFirstCount > bookTickerFirstCount ? 'Depth5' : 'BookTicker';
          console.log(`⚡ 结论: ${winner} 总体更快，但有交替情况`);
        }
      }
    } else {
      console.log('⏳ 等待更多匹配的updateId以进行时序分析...');
    }
    
    console.log('================================================\n');
  }
}, 10000); // 每10秒统计一次

// 错误处理
depthWs.on('error', (err) => {
  console.error('Depth5 WebSocket 错误:', err);
});

bookTickerWs.on('error', (err) => {
  console.error('BookTicker WebSocket 错误:', err);
});

// 关闭处理
depthWs.on('close', () => {
  console.log('Depth5 WebSocket 连接已关闭');
});

bookTickerWs.on('close', () => {
  console.log('BookTicker WebSocket 连接已关闭');
});

// 程序退出时的最终统计
process.on('SIGINT', () => {
  console.log('\n\n🏁 最终时序分析报告:');
  console.log(`📊 Depth5 总推送次数: ${depthPushCount}`);
  console.log(`⚡ BookTicker 总推送次数: ${bookTickerPushCount}`);
  console.log(`🎯 匹配的 updateId 总数: ${matchedUpdateIds.length}`);
  
  const totalTimedPairs = depthFirstCount + bookTickerFirstCount;
  if (totalTimedPairs > 0) {
    console.log('\n=== 时序性能分析 ===');
    console.log(`⚡ BookTicker 先到: ${bookTickerFirstCount}次 (${(bookTickerFirstCount/totalTimedPairs*100).toFixed(1)}%)`);
    console.log(`📊 Depth5 先到: ${depthFirstCount}次 (${(depthFirstCount/totalTimedPairs*100).toFixed(1)}%)`);
    
    // 完整的时间差分析
    const completeTimings = timingAnalysis.filter(t => t.timeDiff !== null);
    if (completeTimings.length > 0) {
      const timeDiffs = completeTimings.map(t => Math.abs(t.timeDiff));
      const avgDiff = timeDiffs.reduce((a, b) => a + b, 0) / timeDiffs.length;
      const maxDiff = Math.max(...timeDiffs);
      const minDiff = Math.min(...timeDiffs);
      
      console.log(`⏱️  平均时间差: ${avgDiff.toFixed(2)}ms`);
      console.log(`📏 时间差范围: ${minDiff}ms - ${maxDiff}ms`);
    }
    
    // 最终结论
    console.log('\n=== 最终结论 ===');
    if (Math.abs(depthFirstCount - bookTickerFirstCount) <= totalTimedPairs * 0.1) {
      console.log('🔄 两个流基本同步，没有固定的先后顺序');
    } else if (bookTickerFirstCount > depthFirstCount) {
      const advantage = (bookTickerFirstCount / totalTimedPairs * 100).toFixed(1);
      console.log(`⚡ BookTicker 总体更快 (${advantage}% 的时间先到)`);
    } else {
      const advantage = (depthFirstCount / totalTimedPairs * 100).toFixed(1);
      console.log(`📊 Depth5 总体更快 (${advantage}% 的时间先到)`);
    }
    
    // 模式分析
    if (alternatingPattern.length >= 5) {
      const patternSummary = alternatingPattern.join('');
      console.log(`🔄 完整时序模式: ${patternSummary}`);
    }
  } else {
    console.log('❌ 未收集到足够的时序对比数据');
  }
  
  // 关闭文件流
  logFile.end();
  csvFile.end();
  
  console.log('\n📁 数据已保存到文件:');
  console.log('  - updateid_timing.log: 原始日志格式');
  console.log('  - updateid_data.csv: CSV格式，便于分析');
  
  process.exit(0);
});