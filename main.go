package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"pulsar-test/producer"
)

func main() {
	//opts := pulsar.ClientOptions{URL: "pulsar://10.102.6.53:6650"}
	opts := pulsar.ClientOptions{URL: "pulsar://172.17.0.8:6650"}
	//pulsar://zc-pulsar-1-broker-0.zc-pulsar-1-broker.my-namespace.svc.cluster.local:6650
	client, err := pulsar.NewClient(opts)
	if err != nil {
		fmt.Printf("create client err %s", err)
		return
	}
	topic := "pulsar-test-1"
	//sub1 := "sub1"
	//sub2 := "sub2"
	producer.Produce(client, topic)
	//go consumer.Consume(client, topic, sub1, 1)
	//go consumer.Consume(client, topic, sub2, 6000)
	//msgID := producer.ProduceForSeek(client, topic)
	//consumer.Seek(client, topic, sub1, msgID)
	//time.Sleep(30 * time.Second)
}
