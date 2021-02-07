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

const basicTimeLayout = "2006/01/02 15:04:05.999"

func main() {
	start := time.Now()

	cmd.LoadAndValidateArgs()

	printBeginBanner()

	totalReadCount, filteredCount := readKafkaAndFilterMsg()

	printSummaryBanner(totalReadCount, filteredCount, time.Since(start))
}

func readKafkaAndFilterMsg() (int, int) {
	openOutputFile()

	msgChan := make(chan kafka.Msg)
	ctx, cancel := context.WithCancel(context.Background())
	beginReadingKafka(ctx, msgChan)

	// print "Waiting..."
	tick := startWaitingTick()

	totalReadCount, filteredCount := 0, 0
	for msg := range msgChan {
		totalReadCount++

		if isFiltered(&msg) {
			WriteFilteredMsg(&msg)

			filteredCount++
			if filteredCount == cmd.Args.Limit {
				cancel()
				break
			}
		}
	}
	tick.Stop()
	return totalReadCount, filteredCount
}

var output *os.File

func openOutputFile() {
	var err error
	output, err = os.OpenFile(cmd.Args.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open result file. output:%s err:%s\n", cmd.Args.Output, err)
	}
}

func startWaitingTick() *time.Ticker {
	tick := time.NewTicker(time.Second * 2)
	go func() {
		fmt.Print("Waiting..")
		for _ = range tick.C {
			fmt.Print(".")
		}
	}()
	return tick
}

func beginReadingKafka(ctx context.Context, msgChan chan kafka.Msg) {
	c := kafka.NewKafkaConsumer(cmd.Args.BrokerServers, cmd.Args.Topic, cmd.Args.UserName, cmd.Args.Password)

	c.ReadMessages(ctx, msgChan, cmd.Args.PollTimeout)
}

func WriteFilteredMsg(msg *kafka.Msg) {
	val := strings.TrimSpace(string(msg.Value))
	if cmd.Args.IsOnlyMsg {
		fmt.Fprintln(output, val)
	} else {
		fmt.Fprintf(output, "time:%s topic:%s partition:%d offset:%d key:%s headers:%v msg:%s\n",
			msg.Time.Format(basicTimeLayout),
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Key,
			msg.Headers,
			val)
	}
}

func isFiltered(msg *kafka.Msg) bool {
	if msg.Time.Before(cmd.Args.StartTime) || msg.Time.After(cmd.Args.EndTime) {
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

func printBeginBanner() {
	fmt.Println("\n==================== BEGIN ====================")
	fmt.Printf(":: Broker Servers: [%s]\n", strings.Join(cmd.Args.BrokerServers, ", "))
	if cmd.Args.UserName != "" {
		fmt.Printf(":: Kafka SASL Plain UserName: %v\n", cmd.Args.UserName)
		fmt.Printf(":: Kafka SASL Plain Password: %v\n", cmd.Args.Password)
	}
	fmt.Println(":: Topic:", cmd.Args.Topic)
	fmt.Printf(":: Partition: %d\n", cmd.Args.Partition)
	fmt.Printf(":: Msg StartTime: %s\n", cmd.Args.StartTime.Format(basicTimeLayout))
	fmt.Printf(":: Msg EndTime: %s\n", cmd.Args.EndTime.Format(basicTimeLayout))
	fmt.Printf(":: StartOffset: %d\n", cmd.Args.StartOffset)
	fmt.Printf(":: EndOffset: %d\n", cmd.Args.EndOffset)
	fmt.Printf(":: Filtered Text: %s\n", cmd.Args.FilterText)
	fmt.Printf(":: Kafka Key: %s\n", cmd.Args.Key)
	fmt.Printf(":: Kafka Header: %v\n", cmd.Args.Headers)
	fmt.Printf(":: Filtered Limit: %d\n", cmd.Args.Limit)
	fmt.Printf(":: Kafka Poll Timeout: %d sec\n", cmd.Args.PollTimeout)
	fmt.Printf(":: Is Only msg: %v\n", cmd.Args.IsOnlyMsg)
	fmt.Print("=================================================\n\n")
}

func printSummaryBanner(total int, count int, elapsed time.Duration) {
	fmt.Println("\n==================== SUMMARY ====================")
	fmt.Println(":: Total:", total)
	fmt.Println(":: Filtered:", count)
	fmt.Println(":: Topic:", cmd.Args.Topic)
	fmt.Println(":: Output:", cmd.Args.Output)
	fmt.Printf(":: Elapsed:%.3f sec\n", elapsed.Seconds())
	fmt.Println("=================================================")
}
