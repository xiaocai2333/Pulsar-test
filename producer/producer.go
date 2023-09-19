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
	//defer wg.Done()
	opts := pulsar.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(opts)
	if err != nil {
		fmt.Printf("create producer err %s", err)
	}
	msgSize := 3 * 1024 * 1024
	strBytes := make([]byte, msgSize)
	for j := 0; j < msgSize; j++ {
		strBytes[j] = byte(j)
	}

	timer := time.NewTimer(60 * time.Minute)
	defer timer.Stop()
	goroutineNum := 6
	//var wg sync.WaitGroup
	for i := 0; i < goroutineNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := &pulsar.ProducerMessage{
				Payload:    strBytes,
				Properties: map[string]string{},
			}
			start := time.Now().UnixMilli()
			for j := 0; j < 100; j++ {
				_, err = producer.Send(context.Background(), msg)
				if err != nil {
					fmt.Printf("producer send error %s", err)
				}
			}
			end := time.Now().UnixMilli()
			fmt.Printf("send 100 msg cost: %d, %f\n", end-start, float64(int64(100*msgSize)/(end-start))*1000.0/1024/1024)
		}()
	}
	wg.Wait()
}

func ProduceForSeek(client pulsar.Client, topic string) pulsar.MessageID {
	//defer wg.Done()
	opts := pulsar.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(opts)
	if err != nil {
		fmt.Printf("create producer err %s", err)
		panic(err)
	}
	i := 0
	//timer := time.NewTimer(60 * time.Minute)
	//defer timer.Stop()
	fmt.Println("produce data...")
	var msgID2 pulsar.MessageID
	for {
		i++
		str := "ahahahaha" + strconv.Itoa(i)
		fmt.Printf("i := %d, insert data: %s \n", i, str)
		msg := &pulsar.ProducerMessage{
			Payload:    []byte(str),
			Properties: map[string]string{},
		}
		msgID, err := producer.Send(context.Background(), msg)
		if err != nil {
			fmt.Printf("producer send error %s", err)
			panic(err)
		}
		if i == 10 {
			msgID2 = msgID
		}
		if i == 20 {
			break
		}
	}
	fmt.Println("msgID2", msgID2)
	return msgID2
}
