package consumer

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

//var client pulsar.Client

func main() {
	opts := pulsar.ClientOptions{URL: "pulsar://localhost:6650"}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		fmt.Printf("create client err %s", err)
	}
	Consume(client, "output-api", "xxxx", 60)
}

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
		case <-consumer.Chan():
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
