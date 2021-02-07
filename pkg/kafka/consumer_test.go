package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestKafkaConsumerHappy(t *testing.T) {
	consumer := NewKafkaConsumer([]string{"dev-tuyy0-cassandra001-ncl.nfra.io:9092"},
		"mytest1",
		"",
		"")

	msgChan := make(chan Msg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)

	// ReadMsg
	consumer.ReadMessages(ctx, msgChan, 100)

	var result []Msg

	cnt := 0
	for msg := range msgChan {
		result = append(result, msg)
		if cnt > 1000 { // 로그를 최대 10건만 읽는다.
			cancel()
		}
		cnt++
	}

	if len(result) == 0 {
		t.Fatalf("invalid result count. cnt:%d\n", len(result))
	}

	for _, m := range result {
		fmt.Printf("time:%s partition:%d offset:%d key:%s header:%v\n",
			m.Time,
			m.Partition,
			m.Offset,
			string(m.Key),
			m.Headers)
	}
}
