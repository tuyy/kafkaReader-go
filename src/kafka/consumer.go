package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic string) *Consumer {
	consumer := &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			SessionTimeout: time.Second * 10,
		}),
	}
	return consumer
}

type Msg struct {
	kafka.Message
}

func NewMsg(m kafka.Message) Msg {
	return Msg{
		m,
	}
}

func (c *Consumer) ReadMessage(ctx context.Context, msgChan chan Msg, pollTimeout int) {
	go func() {
		defer c.reader.Close()

		for {
			newCtx, _ := context.WithTimeout(ctx, time.Second * time.Duration(pollTimeout))

			m, err := c.reader.ReadMessage(newCtx)
			if err != nil {
				log.Println(err)
				break
			}
			msgChan <- NewMsg(m)
		}
		close(msgChan)
	}()
}

