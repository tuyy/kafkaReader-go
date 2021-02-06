package main

import (
	"context"
	"fmt"
	"github.com/tuyy/kafkaReader-go/src/cmd"
	"github.com/tuyy/kafkaReader-go/src/kafka"
	"log"
	"os"
	"strings"
	"time"
)

const summaryTimeLayout = "[2006/01/02 15:04:05.999]"

func main() {
	start := time.Now()

	// TODO 1) kafka username,password 추가
	//      2) 설정파일로 옵션 넣기
	//      3) headers 검증
	//      4) Makefile bin 생성 스크립트 추가
	cmd.LoadAndValidateArgs()

	f, err := os.OpenFile(cmd.Args.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open result file. output:%s err:%s\n", cmd.Args.Output, err)
	}

	c := kafka.NewKafkaConsumer(cmd.Args.BrokerServers, cmd.Args.Topic)
	msgChan := make(chan kafka.Msg)
	ctx, cancel := context.WithCancel(context.Background())

	c.ReadMessages(ctx, msgChan, cmd.Args.PollTimeout)

	fmt.Println("Waiting......")

	total, count := 0, 0
	for msg := range msgChan {
		total++

		if isFiltered(&msg) {
			WriteFilteredMsg(&msg, f)

			count++
			if count == cmd.Args.Limit {
				cancel()
				break
			}
		}
	}

	PrintSummary(total, count, start)
}

func WriteFilteredMsg(msg *kafka.Msg, f *os.File) {
	val := strings.TrimSpace(string(msg.Value))
	if cmd.Args.IsOnlyMsg {
		fmt.Fprintln(f, val)
	} else {
		fmt.Fprintf(f, "time:%s topic:%s partition:%d offset:%d key:%s headers:%v msg:%s\n",
			msg.Time.Format(summaryTimeLayout),
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Key,
			msg.Headers,
			val)
	}
}

func isFiltered(msg *kafka.Msg) bool {
	if msg.Time.Before(cmd.Args.StartTime) || msg.Time.After(cmd.Args.EndTime){
		return false
	}
	if cmd.Args.StartOffset > msg.Offset || cmd.Args.EndOffset < msg.Offset {
		return false
	}
	if cmd.Args.Key != "" && cmd.Args.Key != string(msg.Key) {
		return false
	}
	if cmd.Args.Partition != -1 && cmd.Args.Partition != msg.Partition {
		return false
	}
	if cmd.Args.FilterText != "" && !strings.Contains(string(msg.Value), cmd.Args.FilterText) {
		return false
	}

	if len(cmd.Args.Headers) > 0 {
		for key, val := range cmd.Args.Headers {
			isOk := false
			for _, header := range msg.Headers {
				if header.Key == key && string(header.Value) == val {
					isOk = true
					break
				}
			}
			if !isOk {
				return false
			}
		}
	}

	return true
}

func PrintSummary(total int, count int, start time.Time) {
	fmt.Println("========== summary ==========")
	fmt.Println(":: Total:", total)
	fmt.Println(":: Filtered:", count)
	fmt.Println(":: Filtered Text:", cmd.Args.FilterText)
	fmt.Println(":: Target Topic:", cmd.Args.Topic)
	fmt.Println(":: Output:", cmd.Args.Output)
	fmt.Printf(":: Elapsed:%.3f sec\n", time.Since(start).Seconds())
	fmt.Println("=============================")
}
