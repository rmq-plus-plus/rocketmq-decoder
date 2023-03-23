package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rmq-plus-plus/rocketmq-decoder/common"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	var config = loadConfig()
	if strings.EqualFold(config.Exec, "ReadFromConsumeOffset") {
		readFromConsumeOffset(config.RocketmqDataHome, config.ReadFromConsumeOffset)
	} else if strings.EqualFold(config.Exec, "ReadCommitLogByOffset") {
		readCommitLogByOffset(config.RocketmqDataHome, config.ReadCommitLogByOffset.CommitLogOffset)
	}
}

type Config struct {
	RocketmqDataHome      string
	Exec                  string
	ReadFromConsumeOffset ReadFromConsumeOffset
	ReadCommitLogByOffset ReadCommitLogByOffset
}
type ReadFromConsumeOffset struct {
	QueueOffset uint64
	Topic       string
	QueueId     uint8
}
type ReadCommitLogByOffset struct {
	CommitLogOffset uint64
}

func loadConfig() *Config {
	var configPath string
	argsCount := len(os.Args)
	if argsCount > 0 {
		curDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		configPath = curDir + "/config.json"
	}
	if argsCount > 1 {
		configPath = os.Args[1]
	}

	f, err := os.Open(configPath)
	if err != nil {
		fmt.Println("打开配置文件失败", err)
		return nil
	}
	defer f.Close()

	configBytes, err2 := io.ReadAll(f)
	if len(configBytes) <= 0 {
		fmt.Println("读取配置文件失败", err2)
		return nil
	}
	if len(configBytes) <= 0 {
		fmt.Println("配置文件内容为空")
		return nil
	}
	config := &Config{}
	err3 := json.Unmarshal(configBytes, &config)
	if err3 != nil {
		fmt.Println("Unmarshal配置文件内容失败", err3)
		return nil
	}
	return config
}
func readFromConsumeOffset(rootPath string, config ReadFromConsumeOffset) {
	var consumeQueueDataPath = rootPath + "/consumequeue"
	var queueFilePath = consumeQueueDataPath + "/" + config.Topic + "/" + strconv.Itoa(int(config.QueueId))
	var theFilePath, err1 = getFileNameByOffset(queueFilePath, config.QueueOffset)
	if err1 != nil {
		fmt.Println(err1)
	}

	f, err := os.Open(queueFilePath + "/" + theFilePath)
	if err != nil {
		fmt.Printf("读取consume queue错误 %s, 错误: %v", queueFilePath+"/"+theFilePath, err)
		return
	}
	defer f.Close()

	byteArr4 := make([]byte, 4)
	byteArr8 := make([]byte, 8)

	f.Seek(int64(config.QueueOffset*20), 0)

	kvs := map[string]string{}
	var commitLogOffset uint64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &commitLogOffset)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["commit log offset in message queue"] = fmt.Sprintf("%d", commitLogOffset)
	}

	var totalSize uint32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &totalSize)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message total size in message queue"] = fmt.Sprintf("%d", totalSize)
	}

	var tagsCode uint64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &tagsCode)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message tags hashcode in message queue"] = fmt.Sprintf("%d", tagsCode)
	}

	if totalSize == 0 {
		fmt.Println("消息不存在")
		return
	}

	fmt.Println("============consumer queue info start======================")
	for k, v := range kvs {
		fmt.Println(fmt.Sprintf("%s = %v", k, v))
	}

	fmt.Println("============consumer queue info end======================")

	readCommitLogByOffset(rootPath, commitLogOffset)
}

func getFileNameByOffset(dataPath string, offset uint64) (string, error) {
	fileInfos, err1 := ioutil.ReadDir(dataPath)
	if err1 != nil {
		str := fmt.Sprintf("读取%s目录下全部文件出错： %v \n", dataPath, err1)
		return "", errors.New(str)
	}

	var logNames []string
	for _, fileInfo := range fileInfos {
		logNames = append(logNames, fileInfo.Name())
	}

	var theFileName string

	OrderByDesc(&logNames)
	for _, fileName := range logNames {
		minOffset, err2 := getFileNameAsString(fileName)
		if err2 != nil {
			str := fmt.Sprintf("%s转化为数字offset时报错: %v \n", fileName, err2)
			return "", errors.New(str)
		}
		if offset >= minOffset {
			theFileName = fileName
			return theFileName, nil
		}
	}
	return theFileName, errors.New("NOT_FOUND")
}

func getFileNameAsString(fileName string) (uint64, error) {
	var s2 string
	var found = false

	for _, c := range fileName {
		if c > 48 {
			found = true
			s2 = s2 + string(c)
		}
		if c == 48 && found {
			s2 = s2 + string(c)
		}
	}

	if s2 == "" {
		s2 = "0"
	}
	return strconv.ParseUint(s2, 10, 64)
}
func OrderByDesc(arr *[]string) {
	var temp string
	length := len(*arr)
	for i := 0; i < length/2; i++ {
		temp = (*arr)[i]
		(*arr)[i] = (*arr)[length-1-i]
		(*arr)[length-1-i] = temp
	}
}

func readCommitLogByOffset(rootPath string, commitLogOffset uint64) {
	var commitLogDataPath = rootPath + "/store/commitlog/"

	theFileName, err2 := getFileNameByOffset(commitLogDataPath, commitLogOffset)
	if err2 != nil {
		fmt.Println(err2)
		return
	}

	f, err := os.Open(commitLogDataPath + theFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	byteArr1 := make([]byte, 1)
	byteArr2 := make([]byte, 2)
	byteArr4 := make([]byte, 4)
	byteArr8 := make([]byte, 8)

	kvs := map[string]string{}
	f.Seek(int64(commitLogOffset), 0)
	// total size
	var totalSize int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &totalSize)
		if err != nil {
			fmt.Println(err.Error())
		}
		if totalSize <= 0 {
			fmt.Println("已经读取完毕")
		}
		kvs["message total size"] = fmt.Sprintf("%d", totalSize)
	}

	var magicCode int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &magicCode)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message magic code"] = fmt.Sprintf("%d", magicCode)
	}

	var bodyCrc int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &bodyCrc)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message body crc"] = fmt.Sprintf("%d", bodyCrc)
	}

	var queueId int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &queueId)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message queue id"] = fmt.Sprintf("%d", queueId)
	}

	var flag int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &flag)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message flag"] = fmt.Sprintf("%d", flag)
	}

	var queueOffset int64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &queueOffset)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message queue offset"] = fmt.Sprintf("%d", queueOffset)
	}

	var pyOffset int64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &pyOffset)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message commit log offset"] = fmt.Sprintf("%d", pyOffset)
	}

	var sysFlag int32
	{
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &sysFlag)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message sysFlag"] = fmt.Sprintf("%d", sysFlag)
	}

	var bornTs int64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &bornTs)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message born time"] = common.FormatTimestamp(bornTs)
	}

	{
		var bornPort uint32
		f.Read(byteArr4)
		kvs["message born host"] = net.IPv4(byteArr4[0], byteArr4[1], byteArr4[2], byteArr4[3]).String()

		f.Read(byteArr4)
		buf1 := bytes.NewReader(byteArr4)
		binary.Read(buf1, binary.BigEndian, &bornPort)

		kvs["message born port"] = fmt.Sprintf("%d", bornPort)
	}

	var storeTs int64
	{
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &storeTs)
		if err != nil {
			fmt.Println(err.Error())
		}
		kvs["message store time"] = common.FormatTimestamp(storeTs)
	}

	{
		f.Read(byteArr4)
		kvs["message store host"] = net.IPv4(byteArr4[0], byteArr4[1], byteArr4[2], byteArr4[3]).String()

		var storePort uint32
		f.Read(byteArr4)
		buf1 := bytes.NewReader(byteArr4)
		binary.Read(buf1, binary.BigEndian, &storePort)

		kvs["message store port"] = fmt.Sprintf("%d", storePort)
	}

	{
		var reconsumeTimes uint32

		f.Read(byteArr4)
		buf1 := bytes.NewReader(byteArr4)
		binary.Read(buf1, binary.BigEndian, &reconsumeTimes)

		kvs["message reconsume times"] = fmt.Sprintf("%d", reconsumeTimes)
	}

	{
		var transOffset uint64

		f.Read(byteArr8)
		buf1 := bytes.NewReader(byteArr8)
		binary.Read(buf1, binary.BigEndian, &transOffset)

		kvs["message prepared transaction offset"] = fmt.Sprintf("%d", transOffset)
	}

	{
		var bodyLength uint32
		f.Read(byteArr4)
		buf1 := bytes.NewReader(byteArr4)
		binary.Read(buf1, binary.BigEndian, &bodyLength)

		var leng = int(bodyLength)
		kvs["message body length"] = fmt.Sprintf("%d", leng)

		byteArr := make([]byte, bodyLength)

		f.Read(byteArr)
		kvs["message body"] = string(byteArr)
	}

	{
		var topicLength uint8

		f.Read(byteArr1)
		buf1 := bytes.NewReader(byteArr1)
		binary.Read(buf1, binary.BigEndian, &topicLength)

		kvs["message topic length"] = fmt.Sprintf("%d", topicLength)

		byteArr := make([]byte, int(topicLength))

		f.Read(byteArr)
		kvs["message topic"] = string(byteArr)

	}

	{
		var propertyLength uint16

		f.Read(byteArr2)
		buf1 := bytes.NewReader(byteArr2)
		binary.Read(buf1, binary.BigEndian, &propertyLength)

		kvs["message property length"] = fmt.Sprintf("%d", propertyLength)
		if propertyLength > 0 {
			byteArr := make([]byte, int(propertyLength))
			f.Read(byteArr)
			var a []byte
			a = append(a, 2)
			var b []byte
			b = append(b, 1)
			pkvs := bytes.Split(byteArr, a)
			if len(pkvs) > 0 {
				for k := range pkvs {
					kv := pkvs[k]
					kvBytes := bytes.Split(kv, b)
					kvs["message property "+string(kvBytes[0])] = string(kvBytes[1])
				}
			}
		}
	}

	fmt.Println("============commit log message start======================")
	for k, v := range kvs {
		fmt.Println(fmt.Sprintf("%s = %v", k, v))
	}
	fmt.Println("============commit log message end======================")
}
