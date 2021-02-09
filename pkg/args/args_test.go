package args

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
	// TODO 파일 위치 기준으로 상대경로로 지정
	result, err := readConfigFile("/Users/yy/tuyy/go/src/github.com/tuyy/kafkaReader-go/dist/conf/sample.json")
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
