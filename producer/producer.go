package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "test_topic"
	// partition := 0

	// Set up the writer

	writer := kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	// Write messages

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("Hellow Kafka %d", i)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("Key=%d", i)),
				Value: []byte(msg),
			},
		)

		if err != nil {
			log.Fatalf("Failed to write message: %v", err)
		}
		fmt.Printf("Produced message:%s \n", msg)
		time.Sleep(time.Second) // Simulate a Delay

	}

}
