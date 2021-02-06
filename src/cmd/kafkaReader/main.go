package main

import (
	"flag"
	"log"
	"math"
	"strings"
	"time"
)

const timeLayout = "200601021504"

type CmdArgs struct {
	BrokerServers []string
	Topic         string
	Output        string
	StartTime     time.Time
	EndTime       time.Time
	StartOffset   int
	EndOffset     int
	Limit         int
	FilterText    string
	MaxTimeout    int
}

var cmdArgs CmdArgs

func loadArgs() {
	flag.StringVar(&cmdArgs.Topic, "topic", "", "kafka topic")
	flag.StringVar(&cmdArgs.Output, "output", "result.log", "output file path")
	flag.StringVar(&cmdArgs.FilterText, "grep", "", "included text in msg")
	flag.IntVar(&cmdArgs.Limit, "limit", math.MaxInt32, "max filtered msg limit")
	flag.IntVar(&cmdArgs.MaxTimeout, "maxtimeout", math.MaxInt32, "max timeout")
	flag.IntVar(&cmdArgs.StartOffset, "startoffset", 0, "start offset")
	flag.IntVar(&cmdArgs.EndOffset, "endoffset", math.MaxInt32, "end offset")
	brokerServerPtr := flag.String("b", "", "broker servers")
	startDatePtr := flag.String("start", "199102010308", "start datetime str ex)199102010308")
	endDatePtr := flag.String("end", "299102010308", "end datetime str ex)299102010308")

	flag.Parse()

	if *brokerServerPtr == "" {
		log.Fatalln("empty broker server.")
	}
	for _, brokerServer := range strings.Split(*brokerServerPtr, ",") {
		cmdArgs.BrokerServers = append(cmdArgs.BrokerServers, brokerServer)
	}
	if len(cmdArgs.BrokerServers) == 0 {
		log.Fatalln("empty broker server.")
	}

	if cmdArgs.Topic == "" {
		log.Fatalln("empty input topic.")
	}

	var err error
	cmdArgs.StartTime, err = time.Parse(timeLayout, *startDatePtr)
	if err != nil {
		log.Fatalf("invalid start time. err:%s ex)202102031421\n", err)
	}

	cmdArgs.EndTime, err = time.Parse(timeLayout, *endDatePtr)
	if err != nil {
		log.Fatalf("invalid end time. err:%s ex)202102031421\n", err)
	}

	if cmdArgs.StartTime.After(cmdArgs.EndTime) {
		log.Fatalln("invalid start and end time.")
	}
}

func main() {
	loadArgs()

}
