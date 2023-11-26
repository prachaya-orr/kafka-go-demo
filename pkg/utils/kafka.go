package utils

import (
	"context"
	"log"

	"github.com/prachaya-orr/kafka-go-demo/config"
	"github.com/segmentio/kafka-go"
)

func KafkaConn(cfg config.KafkaConfig) *kafka.Conn {
	// to produce messages
	url := cfg.Url
	topic := cfg.Topic
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", url, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return conn
}

func IsTopicAlreadyExists(conn *kafka.Conn, topic string) bool {
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		panic(err.Error())
	}

	for _, p := range partitions {
		if p.Topic == topic {
			return true
		}
	}
	return false
}
