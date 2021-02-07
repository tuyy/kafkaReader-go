package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"time"
)

const timeLayout = "200601021504"

type KafkaReaderArgs struct {
	BrokerServers []string
	Topic         string
	Partition     int               `json:",omitempty"`
	Key           string            `json:",omitempty"`
	Headers       map[string]string `json:",omitempty"`
	UserName      string            `json:",omitempty"`
	Password      string            `json:",omitempty"`
	StartOffset   int64             `json:",omitempty"`
	EndOffset     int64             `json:",omitempty"`
	Limit         int               `json:",omitempty"`
	PollTimeout   int               `json:",omitempty"`
	StartTime     time.Time         `json:",omitempty"`
	EndTime       time.Time         `json:",omitempty"`
	FilterText    string            `json:",omitempty"`
	Output        string            `json:",omitempty"`
	IsOnlyMsg     bool              `json:",omitempty"`
}

var Args KafkaReaderArgs

func LoadAndValidateArgs() {
	flag.StringVar(&Args.Topic, "topic", "", "kafka topic")
	flag.StringVar(&Args.Output, "output", "result.log", "output file path")
	flag.StringVar(&Args.FilterText, "grep", "", "included text in msg")
	flag.StringVar(&Args.Key, "key", "", "kafka key. default:ignore")
	flag.StringVar(&Args.UserName, "username", "", "kafka sasl plain username")
	flag.StringVar(&Args.Password, "password", "", "kafka sasl plain password")
	flag.BoolVar(&Args.IsOnlyMsg, "onlymsg", false, "include only msg")
	flag.IntVar(&Args.Partition, "partition", -1, "partition")
	flag.IntVar(&Args.Limit, "limit", math.MaxInt32, "max filtered msg limit")
	flag.IntVar(&Args.PollTimeout, "polltimeout", 60, "kafka msg poll timeout")
	flag.Int64Var(&Args.StartOffset, "startoffset", 0, "start offset")
	flag.Int64Var(&Args.EndOffset, "endoffset", math.MaxInt32, "end offset")
	brokerServerPtr := flag.String("b", "", "broker servers")
	headerPtr := flag.String("headers", "", "kafka headers with json string")
	startDatePtr := flag.String("start", "199102010000", "start datetime str ex)199102010308")
	endDatePtr := flag.String("end", "204102010000", "end datetime str ex)202502010308")
	configFilePtr := flag.String("config", "", "conf file path")
	flag.Parse()

	validateArgs(*brokerServerPtr, *headerPtr, *startDatePtr, *endDatePtr, *configFilePtr)

	writeInputArgsInHistoryDir()
}

const historyDir = "history"
const inputConfFileFormat = "input_*_%s.json"

func writeInputArgsInHistoryDir() {
	b, err := json.MarshalIndent(Args, "", "    ")
	if err != nil {
		log.Fatalf("failed to write input args in history dir. err:%s\n", err)
	}

	_ = os.Mkdir(historyDir, os.ModePerm)

	tmpFilePattern := fmt.Sprintf(inputConfFileFormat, time.Now().Format("2006-01-02_150405"))
	f, err := ioutil.TempFile(historyDir, tmpFilePattern)
	if err != nil {
		log.Fatalf("failed to create tempfile. err:%s\n", err)
	}
	defer f.Close()

	fmt.Fprintln(f, string(b))
}

func validateArgs(brokerServers, header, startDate, endDate, configFile string) {
	validateStartTime(startDate)
	validateEndTime(endDate)

	if configFile != "" {
		loadConfigFile(configFile)
	}

	validateKafkaBrokerServers(brokerServers)
	validateKafkaHeaders(header)
	validateKafkaTopic()
	validateTimes()
}

func loadConfigFile(configFileStr string) {
	newConf, err := readConfigFile(configFileStr)
	if err != nil {
		log.Fatal(err)
	}

	if len(newConf.BrokerServers) > 0 {
		Args.BrokerServers = newConf.BrokerServers
	}
	if newConf.Topic != Args.Topic {
		Args.Topic = newConf.Topic
	}
	if newConf.UserName != Args.UserName {
		Args.UserName = newConf.UserName
	}
	if newConf.Password != Args.Password {
		Args.Password = newConf.Password
	}
	if newConf.PollTimeout != Args.PollTimeout {
		Args.PollTimeout = newConf.PollTimeout
	}
	if newConf.FilterText != Args.FilterText {
		Args.FilterText = newConf.FilterText
	}
	if newConf.Output != Args.Output {
		Args.Output = newConf.Output
	}
	if newConf.IsOnlyMsg != Args.IsOnlyMsg {
		Args.IsOnlyMsg = newConf.IsOnlyMsg
	}
	if newConf.Key != Args.Key {
		Args.Key = newConf.Key
	}
	if !reflect.DeepEqual(newConf.Headers, Args.Headers) {
		Args.Headers = newConf.Headers
	}
	if newConf.Limit != Args.Limit {
		Args.Limit = newConf.Limit
	}
	if newConf.StartTime != Args.StartTime {
		Args.StartTime = newConf.StartTime
	}
	if newConf.EndTime != Args.EndTime {
		Args.EndTime = newConf.EndTime
	}
}

func readConfigFile(filename string) (*KafkaReaderArgs, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	result := &KafkaReaderArgs{}
	err = json.Unmarshal(b, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func validateTimes() {
	if Args.StartTime.After(Args.EndTime) {
		log.Fatalln("invalid start and end time.")
	}
}

func validateStartTime(startDate string) {
	var err error
	Args.StartTime, err = time.ParseInLocation(timeLayout, startDate, time.Local)
	if err != nil {
		log.Fatalf("invalid start time. err:%s ex)202102031421\n", err)
	}
}

func validateEndTime(endDate string) {
	var err error
	Args.EndTime, err = time.ParseInLocation(timeLayout, endDate, time.Local)
	if err != nil {
		log.Fatalf("invalid end time. err:%s ex)202102031421\n", err)
	}
}

func validateKafkaTopic() {
	if Args.Topic == "" {
		log.Fatalln("empty input topic.")
	}
}

func validateKafkaHeaders(headers string) {
	if headers != "" {
		err := json.Unmarshal([]byte(headers), &Args.Headers)
		if err != nil {
			log.Fatalf("invalid kafka header. err:%s\n", err)
		}
	} else if len(Args.Headers) == 0 {
		Args.Headers = make(map[string]string)
	}
}

func validateKafkaBrokerServers(brokerServers string) {
	if brokerServers == "" && len(Args.BrokerServers) == 0 {
		log.Fatalln("empty broker server.")
	}
	for _, brokerServer := range strings.Split(brokerServers, ",") {
		if brokerServer != "" {
			Args.BrokerServers = append(Args.BrokerServers, brokerServer)
		}
	}
	if len(Args.BrokerServers) == 0 {
		log.Fatalln("empty broker server.")
	}
}
