package consumer

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func Consume(client pulsar.Client, topic, subName string, timeout time.Duration) {

	receiveCh := make(chan pulsar.ConsumerMessage)

	opts := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        0,
		SubscriptionInitialPosition: 0,
		MessageChannel:              receiveCh,
	}
	consumer, err := client.Subscribe(opts)
	if err != nil {
		fmt.Printf("create consumer err %s", err)
	}
	var startTs time.Time
	timer := time.NewTimer(timeout * time.Second)
	defer timer.Stop()
	n := 0
	for {
		select {
		case msg, ok := <-consumer.Chan():
			if !ok {
				fmt.Println("consumer Chan closed")
				return
			}
			if n%10 == 0 {
				fmt.Println("receive message length: ", len(msg.Message.Payload()))
			}
			msg.Ack(msg)
			n++
			if startTs.IsZero() {
				startTs = time.Now()
			}
		case <-timer.C:
			duration := time.Since(startTs).Seconds()
			fmt.Printf("rate: %.2f", float64(n)/duration)
			return
		}
	}
}

func Seek(client pulsar.Client, topic, subName string, msgID pulsar.MessageID) {
	receiveCh := make(chan pulsar.ConsumerMessage)

	opts := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        0,
		SubscriptionInitialPosition: 0,
		MessageChannel:              receiveCh,
	}
	consumer, err := client.Subscribe(opts)
	if err != nil {
		fmt.Printf("create consumer err %s", err)
	}
	err = consumer.Seek(msgID)
	if err != nil {
		panic(err)
	}
	for {
		select {
		case msg, ok := <-consumer.Chan():
			if !ok {
				return
			}
			fmt.Println("consumer data", string(msg.Message.Payload()))
		}
	}
}
