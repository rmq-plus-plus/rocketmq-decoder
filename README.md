## 本项目支持解析rocketmq commit log文件和consumer queue文件

## 如何使用
目前支持以下两种读取方式
- 解析commit log文件
- 解析consumer queue文件

配置使用如下：
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

## 解析结果参考
<img src="https://raw.githubusercontent.com/rmq-plus-plus/rocketmq-decoder/main/images/01.png" />