package main

import (
	"context"
	"fmt"
	"github.com/tuyy/kafkaReader-go/pkg/args"
	"github.com/tuyy/kafkaReader-go/pkg/description"
	"github.com/tuyy/kafkaReader-go/pkg/kafka"
	"log"
	"os"
	"strings"
	"time"
)

const basicTimeLayout = "2006/01/02 15:04:05.999"

func main() {
	start := time.Now()

	args.LoadAndValidateArgs()

	printBeginBanner()

	totalReadCount, filteredCount := readKafkaAndFilterMsg()

	printSummaryBanner(totalReadCount, filteredCount, time.Since(start))
}

// TODO 함수가 너무 길다.. 줄여야한다.
func readKafkaAndFilterMsg() (int, int) {
	openOutputFile()
	defer output.Close()

	msgChan := make(chan kafka.Msg)
	ctx, cancel := context.WithCancel(context.Background())
	runReadingKafkaMsg(ctx, msgChan)

	totalReadCount, filteredCount := 0, 0
	for msg := range msgChan {
		totalReadCount++
		if totalReadCount%10000 == 0 {
			fmt.Printf("[%s] TotalReadCount:%d FilteredCount:%d CurrentMsgTime:%s\n",
				time.Now().Local().Format(basicTimeLayout),
				totalReadCount,
				filteredCount,
				msg.Time.Format(basicTimeLayout))
		}

		var payload string
		if args.Args.IsDecrypted {
			decrypted, err := description.DecryptPayload(string(msg.Value))
			if err != nil {
				fmt.Printf("failed to decrypt payload. err:%s payload:%s\n", err, payload)
			}
			payload = strings.TrimSpace(decrypted)
		} else {
			payload = strings.TrimSpace(string(msg.Value))
		}

		if msg.Time.After(args.Args.EndTime) {
			cancel()
			break
		}

		if isFiltered(&msg, payload) {
			WriteFilteredMsg(&msg, payload)

			filteredCount++
			if filteredCount == args.Args.Limit {
				cancel()
				break
			}
		}
	}

	return totalReadCount, filteredCount
}

var output *os.File

func openOutputFile() {
	if args.Args.UseStdout {
		output = os.Stdout
	} else {
		var err error
		output, err = os.OpenFile(args.Args.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("failed to open result file. output:%s err:%s\n", args.Args.Output, err)
		}
	}
}

func runReadingKafkaMsg(ctx context.Context, msgChan chan kafka.Msg) {
	c := kafka.NewConsumer(args.Args.BrokerServers, args.Args.Topic, args.Args.UserName, args.Args.Password)

	c.ReadMessages(ctx, msgChan, args.Args.PollTimeout)
}

func WriteFilteredMsg(msg *kafka.Msg, payload string) {
	if args.Args.IsOnlyMsg {
		fmt.Fprintln(output, payload)
	} else {
		fmt.Fprintf(output, "time:%s topic:%s partition:%d offset:%d key:%s headers:%v msg:%s\n",
			msg.Time.In(time.Local).Format(basicTimeLayout),
			msg.Topic,
			msg.Partition,
			msg.Offset,
			msg.Key,
			msg.Headers,
			payload)
	}
}

func isFiltered(msg *kafka.Msg, payload string) bool {
	if msg.Time.Before(args.Args.StartTime) || msg.Time.After(args.Args.EndTime) {
		return false
	}
	if args.Args.StartOffset > msg.Offset || args.Args.EndOffset < msg.Offset {
		return false
	}
	if args.Args.Key != "" && args.Args.Key != string(msg.Key) {
		return false
	}
	if args.Args.Partition != -1 && args.Args.Partition != msg.Partition {
		return false
	}
	if args.Args.FilterText != "" && !strings.Contains(payload, args.Args.FilterText) {
		return false
	}

	if len(args.Args.Headers) > 0 {
		for key, val := range args.Args.Headers {
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
	fmt.Printf(":: Broker Servers: [%s]\n", strings.Join(args.Args.BrokerServers, ", "))
	if args.Args.UserName != "" {
		fmt.Printf(":: Kafka SASL Plain UserName: %v\n", args.Args.UserName)
		fmt.Printf(":: Kafka SASL Plain Password: %v\n", args.Args.Password)
	}
	fmt.Println(":: Topic:", args.Args.Topic)
	fmt.Printf(":: Partition: %d\n", args.Args.Partition)
	fmt.Printf(":: Msg StartTime: %s\n", args.Args.StartTime.Format(basicTimeLayout))
	fmt.Printf(":: Msg EndTime: %s\n", args.Args.EndTime.Format(basicTimeLayout))
	fmt.Printf(":: StartOffset: %d\n", args.Args.StartOffset)
	fmt.Printf(":: EndOffset: %d\n", args.Args.EndOffset)
	fmt.Printf(":: Filtered Text: %s\n", args.Args.FilterText)
	fmt.Printf(":: Kafka Key: %s\n", args.Args.Key)
	fmt.Printf(":: Kafka Header: %v\n", args.Args.Headers)
	fmt.Printf(":: Filtered Limit: %d\n", args.Args.Limit)
	fmt.Printf(":: Kafka Poll Timeout: %d sec\n", args.Args.PollTimeout)
	fmt.Printf(":: Payload Decrypted: %v\n", args.Args.IsDecrypted)
	if args.Args.IsDecrypted {
		fmt.Printf(":: Payload Decrypt Key: %s\n", args.Args.DecryptKey)
	}
	fmt.Printf(":: Is Only msg: %v\n", args.Args.IsOnlyMsg)
	fmt.Print("=================================================\n\n")
}

func printSummaryBanner(total int, count int, elapsed time.Duration) {
	fmt.Println("\n\n==================== SUMMARY ====================")
	fmt.Println(":: Total:", total)
	fmt.Println(":: Filtered:", count)
	fmt.Println(":: Topic:", args.Args.Topic)
	fmt.Println(":: Output:", args.Args.Output)
	fmt.Printf(":: Elapsed:%.3f sec\n", elapsed.Seconds())
	fmt.Println("=================================================")
}
