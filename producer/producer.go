package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func Produce(client pulsar.Client, topic string, msgSize int, duration time.Duration) {
	//defer wg.Done()
	opts := pulsar.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(opts)
	if err != nil {
		fmt.Printf("create producer err %s", err)
		return
	}
	defer producer.Close()

	strBytes := make([]byte, msgSize)
	for j := 0; j < msgSize; j++ {
		strBytes[j] = byte(j)
	}
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	msg := &pulsar.ProducerMessage{
		Payload:    strBytes,
		Properties: map[string]string{},
	}
	messageNum := 0
	start := time.Now().UnixMilli()
	//var wg sync.WaitGroup
	for {
		select {
		case <-ticker.C:
			end := time.Now().UnixMilli()
			fmt.Printf("send %d msg %d cost: %d, %f\n", messageNum, msgSize, end-start,
				float64(int64(messageNum*msgSize)/(end-start))*1000.0/1024/1024)
			return
		default:
			// ts1 := time.Now().UnixMilli()
			_, err = producer.Send(context.Background(), msg)
			if err != nil {
				fmt.Printf("producer send error %s", err)
				return
			}
			// ts2 := time.Now().UnixMilli()
			// fmt.Printf(" cost: %d", ts2-ts1)
			messageNum++
		}
	}
}
