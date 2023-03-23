## 本项目支持解析rocketmq commit log文件和consumer queue文件

## 如何使用
### 第一步，配置config.json
- 配置config.json说明如下：
```json
{
  "RocketmqDataHome": "RocketMQ数据根目录, 和broker配置中storePathRootDir一样",
  "Exec": "ReadFromConsumeOffset", // 程序启动使用 ReadFromConsumeOffset 或者ReadCommitLogByOffset 配置
  "ReadFromConsumeOffset": { // 按照consumer offset 读取消息
    "Topic": "test-topic", // topic名字
    "QueueId": 1 // topic queue id
    "QueueOffset": 0, //需要读取哪个queue offset的消息
    
  },
  "ReadCommitLogByOffset": { // 直接读取commitlog offset位置的消息
    "CommitLogOffset": 0 // commit log offset
  }
}

```
- 解析commit log offset =0的消息
```json
{
  "RocketmqDataHome": "RocketMQ数据根目录, 和broker配置中storePathRootDir一样",
  "Exec": "ReadCommitLogByOffset",
  "ReadCommitLogByOffset": {
    "CommitLogOffset": 0
  }
}

```
- 解析topic = test-topic，queue id = 1，queue offset = 0的消息 
```json
{
  "RocketmqDataHome": "RocketMQ数据根目录, 和broker配置中storePathRootDir一样",
  "Exec": "ReadFromConsumeOffset",
  "ReadFromConsumeOffset": {
    "Topic": "test-topic", 
    "QueueId": 1,
    "QueueOffset": 0
    
  }
}

```

### 第二步，启动程序
#### 第一种：golang debug启动
在golang中添加设置debug/run的配置文件启动路径
<img src="https://raw.githubusercontent.com/rmq-plus-plus/rocketmq-decoder/main/images/02.png" />

#### 第二种：打包执行
```bash
go build
```
再执行
```bash
./rocketmq-decoder /Users/tigerweili/RocketMQ/rocketmq-decoder/config.json
```

#### 解析结果参考
<img src="https://raw.githubusercontent.com/rmq-plus-plus/rocketmq-decoder/main/images/01.png" />