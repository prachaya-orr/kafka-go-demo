package main

import (
	"fmt"
	"log"

	"github.com/prachaya-orr/kafka-go-demo/config"
	"github.com/prachaya-orr/kafka-go-demo/pkg/utils"
)

func main() {
	//Congfig part
	cfg := config.KafkaConfig{
		Url:   "localhost:29092",
		Topic: "shop",
	}

	conn := utils.KafkaConn(cfg)

	for {
		message, err := conn.ReadMessage(10e3)
		if err != nil {
			break
		}
		fmt.Println(string(message.Value))
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to cloase connection:", err)
	}
}
