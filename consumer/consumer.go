package main

import (
	"context"
	"fmt"
	"log"
	"github.com/segmentio/kafka-go"
)

func main() {
	 topic := "test_topic"
	 partition := 0

	 // Set up the Reader

	 reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: topic,
		Partition: partition,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	 })

	 defer reader.Close()

	 fmt.Println("Consumer is ready. Waiting for messages...")

	 for {
		// Read messages
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Failed to read message: %v", err)
		}
		fmt.Printf("Received message: %s from topic: %s at offset: %d\\n",
	   string(msg.Value), msg.Topic, msg.Offset)
	
	 }
}