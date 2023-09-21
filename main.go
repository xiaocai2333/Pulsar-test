package main

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"os"
	"pulsar-test/producer"
	"strconv"
	"sync"
	"time"
)

func main() {
	//opts := pulsar.ClientOptions{URL: "pulsar://10.102.6.53:6650"}
	args := os.Args
	fmt.Println("program name: ", args[0])
	if len(args) != 6 {
		fmt.Println("invalid args")
		return
	}
	for _, arg := range args {
		fmt.Println("arg: ", arg)
	}
	pulsarAddr := args[1]
	topicPrefix := args[2]
	goroutinesNumStr := args[3]
	durationStr := args[4]
	goroutinesNum, err := strconv.Atoi(goroutinesNumStr)
	sameTopic := args[5]
	if err != nil {
		fmt.Println("goroutineNum invalid: ", err)
		return
	}
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

	var wg sync.WaitGroup
	duration := time.Minute * time.Duration(durationNum)

	for i := 0; i < goroutinesNum; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			topicName := topicPrefix
			if sameTopic == "false" {
				topicName = topicPrefix + "-" + strconv.Itoa(i)
			}
			producer.Produce(client, topicName, duration)
		}()
	}
}
