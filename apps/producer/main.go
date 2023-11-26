package main

import (
	"log"
	"time"

	"github.com/prachaya-orr/kafka-go-demo/config"
	"github.com/prachaya-orr/kafka-go-demo/models"
	"github.com/prachaya-orr/kafka-go-demo/pkg/utils"
	"github.com/segmentio/kafka-go"
)

func main() {
	//Congfig part
	cfg := config.KafkaConfig{
		Url:   "localhost:29092",
		Topic: "shop",
	}

	conn := utils.KafkaConn(cfg)

	if !utils.IsTopicAlreadyExists(conn, cfg.Topic) {
		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             cfg.Topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}

		err := conn.CreateTopics(topicConfigs...)
		if err != nil {
			panic(err)
		}
	}

	data := func() []kafka.Message {
		product := []models.Product{
			{
				Id:    "1",
				Title: "Cofee",
			},
			{
				Id:    "2",
				Title: "Tea",
			},
			{
				Id:    "3",
				Title: "Milk",
			},
		}

		messages := make([]kafka.Message, 0)
		for _, p := range product {
			messages = append(messages, kafka.Message{
				Value: utils.CompressToJson(p),
			})
		}

		return messages
	}()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.WriteMessages(data...)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}
