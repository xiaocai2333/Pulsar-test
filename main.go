package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"os"
	"pulsar-test/consumer"
	"pulsar-test/producer"
	"strconv"
	"sync"
	"time"
)

func main() {
	//opts := pulsar.ClientOptions{URL: "pulsar://10.102.6.53:6650"}
	args := os.Args
	fmt.Println("program name: ", args[0])
	if len(args) != 5 {
		fmt.Println("invalid args")
		return
	}
	for _, arg := range args {
		fmt.Println("arg: ", arg)
	}
	pulsarAddr := args[1]
	topicPrefix := args[2]
	//goroutinesNumStr := args[3]
	durationStr := args[3]
	//goroutinesNum, err := strconv.Atoi(goroutinesNumStr)
	sameTopic := args[4]
	//if err != nil {
	//	fmt.Println("goroutineNum invalid: ", err)
	//	return
	//}
	durationNum, err := strconv.Atoi(durationStr)
	if err != nil {
		fmt.Println("goroutineNum invalid: ", err)
		return
	}
	//opts := pulsar.ClientOptions{URL: "pulsar://172.17.0.8:6650"}
	opts := pulsar.ClientOptions{URL: pulsarAddr}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		fmt.Printf("create client err %s", err)
		return
	}

	msgSizeList := []int{1 * 1024, 500 * 1024, 1 * 1024 * 1024, 5 * 1024 * 1024}
	goroutinesNumList := []int{1, 10, 50, 100}

	var wg sync.WaitGroup
	duration := time.Minute * time.Duration(durationNum)

	go consumer.Consume(client, topicPrefix, "sub-1", time.Second*10)

	for _, msgSize := range msgSizeList {
		for _, goroutinesNum := range goroutinesNumList {
			for i := 0; i < goroutinesNum; i++ {
				wg.Add(1)
				i := i
				go func() {
					defer wg.Done()
					topicName := topicPrefix
					if sameTopic == "false" {
						topicName = topicPrefix + "-" + strconv.Itoa(i)
					}
					producer.Produce(client, topicName, msgSize, duration)
				}()
			}
		}
	}
}
