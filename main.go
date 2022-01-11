package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"pulsar-test/consumer"
	"pulsar-test/producer"
)

func main() {
	opts := pulsar.ClientOptions{URL: "pulsar://localhost:6650"}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		fmt.Printf("create client err %s", err)
	}
	topic := "pulsar-test-1"
	sub1 := "sub1"
	sub2 := "sub2"
	go producer.Produce(topic)
	go consumer.Consume(client, topic, sub1, 60)
	go consumer.Consume(client, topic, sub2, 6000)
}


