package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"time"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic, userName, password string) *Consumer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	if len(userName) > 0 {
		dialer.SASLMechanism = plain.Mechanism{
			Username: userName,
			Password: password,
		}
	}

	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			SessionTimeout: time.Second * 10,
			Dialer:         dialer,
		}),
	}
}

type Msg struct {
	kafka.Message
}

func NewMsg(m kafka.Message) Msg {
	return Msg{
		m,
	}
}

func (c *Consumer) ReadMessages(ctx context.Context, msgChan chan Msg, pollTimeout int) {
	go func() {
		defer c.reader.Close()

		for {
			newCtx, _ := context.WithTimeout(ctx, time.Second*time.Duration(pollTimeout))

			m, err := c.reader.ReadMessage(newCtx)
			if err != nil {
				fmt.Printf("\n\n=> Stopped Kafka Consumer. because %s.\n", err)
				break
			}
			msgChan <- NewMsg(m)
		}
		close(msgChan)
	}()
}
