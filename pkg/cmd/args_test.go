package cmd

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfigFileJson(t *testing.T) {
	args := KafkaReaderArgs{}
	b, err := json.MarshalIndent(args, "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}

func TestReadConfigFile(t *testing.T) {
	result, err := readConfigFile("/Users/yy/tuyy/go/src/github.com/tuyy/kafkaReader-go/conf/sample.json")
	if err != nil {
		t.Fatalf("failed to read conf file. err:%s\n", err)
	}
	if result.BrokerServers[0] != "dev-tuyy0-cassandra001-ncl.nfra.io:9092" {
		t.Fatalf("invalid brokerServers. rz:%s\n", result.BrokerServers[0])
	}
	if result.Topic != "mytest1" {
		t.Fatalf("invalid topic. rz:%s\n", result.Topic)
	}
}
