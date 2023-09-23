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
		return
	}
	defer consumer.Close()

	timer := time.NewTicker(timeout)
	defer timer.Stop()
	n := 0
	for {
		select {
		case msg, ok := <-consumer.Chan():
			if !ok {
				fmt.Println("consumer Chan closed")
				return
			}
			msg.Ack(msg)
			n++
		case <-timer.C:
			fmt.Println(fmt.Sprintf("consume msg num: %d", n))
			//return
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
