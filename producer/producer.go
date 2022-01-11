package producer

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var client pulsar.Client
var wg sync.WaitGroup

func main() {
	opts := pulsar.ClientOptions{URL: "pulsar://localhost:6650"}
	var err error
	client, err = pulsar.NewClient(opts)
	if err != nil {
		fmt.Printf("create client err %s", err)
	}
	wg = sync.WaitGroup{}
	cnt := 5
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go Produce(client, "input-api")
	}
	wg.Wait()
}

func Produce(client pulsar.Client, topic string) {
	defer wg.Done()
	opts := pulsar.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(opts)
	if err != nil {
		fmt.Printf("create producer err %s", err)
	}
	i := 0
	timer := time.NewTimer(60 * time.Minute)
	defer timer.Stop()
	for {
		str := "ahahahaha" + strconv.Itoa(i)

		msg := &pulsar.ProducerMessage{
			Payload:    []byte(str),
			Properties: map[string]string{},
		}
		select {
		case <-timer.C:
			return
		default:
		}
		_, err = producer.Send(context.Background(), msg)
		if err != nil {
			fmt.Printf("producer send error %s", err)
		}
		i++
	}
}
