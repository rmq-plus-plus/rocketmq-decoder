package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

var indexFilePath = "/Users/tigerweili/tmp/20231110230801686"

func main() {
	f, err := os.Open(indexFilePath)
	if err != nil {
		fmt.Printf("读取consume queue错误 %s, 错误: %v", indexFilePath)
		return
	}
	defer f.Close()

	byteArr4 := make([]byte, 4)
	byteArr8 := make([]byte, 8)

	{
		var beginTimestamp int64
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &beginTimestamp)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header beginTimestamp=", beginTimestamp, ",")
	}

	{
		var endTimestamp int64
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &endTimestamp)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header endTimestamp=", endTimestamp, "\n")
	}
	{
		var beginPhysicalOffset int64
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &beginPhysicalOffset)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header beginPhysicalOffset=", beginPhysicalOffset, "\n")
	}
	{
		var endPhysicalOffset int64
		f.Read(byteArr8)
		buf := bytes.NewReader(byteArr8)
		err := binary.Read(buf, binary.BigEndian, &endPhysicalOffset)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header endPhysicalOffset=", endPhysicalOffset, "\n")
	}
	{
		var hashSlotCount int32
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &hashSlotCount)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header hashSlotCount=", hashSlotCount, "\n")
	}
	{
		var indexCount int32
		f.Read(byteArr4)
		buf := bytes.NewReader(byteArr4)
		err := binary.Read(buf, binary.BigEndian, &indexCount)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Print("header indexCount=", indexCount, "\n")
	}
	for {
		{
			var hashCode int32
			f.Read(byteArr4)
			buf := bytes.NewReader(byteArr4)
			err := binary.Read(buf, binary.BigEndian, &hashCode)
			if err != nil {
				fmt.Println(err.Error())
				break
			}
			fmt.Print("hashCode=", hashCode, ",")
		}
		{
			var commitLogOffset int64
			f.Read(byteArr8)
			buf := bytes.NewReader(byteArr8)
			err := binary.Read(buf, binary.BigEndian, &commitLogOffset)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Print("commitLogOffset=", commitLogOffset, ",")
		}
		{
			var timeDiff int32
			f.Read(byteArr4)
			buf := bytes.NewReader(byteArr4)
			err := binary.Read(buf, binary.BigEndian, &timeDiff)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Print("timeDiff=", timeDiff, ",")
		}
		{
			var nextIndexPos int32
			f.Read(byteArr4)
			buf := bytes.NewReader(byteArr4)
			err := binary.Read(buf, binary.BigEndian, &nextIndexPos)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Print("nextIndexPos=", nextIndexPos, "\n")
		}

		time.Sleep(time.Second)
	}

}
