package cmd

import (
	"encoding/json"
	"flag"
	"log"
	"math"
	"strings"
	"time"
)

const timeLayout = "200601021504"

type KafkaReaderArgs struct {
	BrokerServers []string
	Topic         string
	Partition     int
	Key           string            // kafka key
	Headers       map[string]string // kafka headers
	StartOffset   int64
	EndOffset     int64
	Limit         int
	PollTimeout   int
	StartTime     time.Time
	EndTime       time.Time
	FilterText    string
	Output        string
	IsOnlyMsg     bool
}

var Args KafkaReaderArgs

func LoadAndValidateArgs() {
	flag.StringVar(&Args.Topic, "topic", "", "kafka topic")
	flag.StringVar(&Args.Output, "output", "result.log", "output file path")
	flag.StringVar(&Args.FilterText, "grep", "", "included text in msg")
	flag.StringVar(&Args.Key, "key", "", "kafka key. default:ignore")
	flag.BoolVar(&Args.IsOnlyMsg, "onlymsg", false, "include only msg")
	flag.IntVar(&Args.Partition, "partition", -1, "partition")
	flag.IntVar(&Args.Limit, "limit", math.MaxInt32, "max filtered msg limit")
	flag.IntVar(&Args.PollTimeout, "polltimeout", 60, "kafka msg poll timeout")
	flag.Int64Var(&Args.StartOffset, "startoffset", 0, "start offset")
	flag.Int64Var(&Args.EndOffset, "endoffset", math.MaxInt32, "end offset")
	brokerServerPtr := flag.String("b", "", "broker servers")
	headerPtr := flag.String("headers", "{}", "kafka headers with json string")
	startDatePtr := flag.String("start", "199102010308", "start datetime str ex)199102010308")
	endDatePtr := flag.String("end", "299102010308", "end datetime str ex)299102010308")
	flag.Parse()

	validateArgs(*brokerServerPtr, *headerPtr, *startDatePtr, *endDatePtr)
}

func validateArgs(brokerServerPtr string, headerPtr string, startDatePtr string, endDatePtr string) {
	validateKafkaBrokerServers(brokerServerPtr)
	validateKafkaHeaders(headerPtr)
	validateKafkaTopic()
	validateStartTime(startDatePtr)
	validateEndTime(endDatePtr)
	validateTimes()
}

func validateTimes() {
	if Args.StartTime.After(Args.EndTime) {
		log.Fatalln("invalid start and end time.")
	}
}

func validateStartTime(startDatePtr string) {
	var err error
	Args.StartTime, err = time.ParseInLocation(timeLayout, startDatePtr, time.Local)
	if err != nil {
		log.Fatalf("invalid start time. err:%s ex)202102031421\n", err)
	}
}

func validateEndTime(endDatePtr string) {
	var err error
	Args.EndTime, err = time.ParseInLocation(timeLayout, endDatePtr, time.Local)
	if err != nil {
		log.Fatalf("invalid end time. err:%s ex)202102031421\n", err)
	}
}

func validateKafkaTopic() {
	if Args.Topic == "" {
		log.Fatalln("empty input topic.")
	}
}

func validateKafkaHeaders(headerPtr string) {
	err := json.Unmarshal([]byte(headerPtr), Args.Headers)
	if err != nil {
		log.Fatalf("invalid kafka header. err:%s\n", err)
	}
}

func validateKafkaBrokerServers(brokerServerPtr string) {
	if brokerServerPtr == "" {
		log.Fatalln("empty broker server.")
	}
	for _, brokerServer := range strings.Split(brokerServerPtr, ",") {
		Args.BrokerServers = append(Args.BrokerServers, brokerServer)
	}
	if len(Args.BrokerServers) == 0 {
		log.Fatalln("empty broker server.")
	}
}
