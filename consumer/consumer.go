package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	 "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const defaultUsername = "$ConnectionString"

func main() {
	kafkaEndpoint := os.Getenv("KAFKA_EVENTHUB_ENDPOINT")
	if kafkaEndpoint == "" {
		log.Fatal("please specify KAFKA_EVENTHUB_ENDPOINT environment variable")
	}
	log.Printf("KAFKA_EVENTHUB_ENDPOINT - %s\n", kafkaEndpoint)

	eventHubConnectionString := os.Getenv("KAFKA_EVENTHUB_PASSWORD")
	if eventHubConnectionString == "" {
		log.Fatal("please specify KAFKA_EVENTHUB_PASSWORD environment variable")
	}

	user := os.Getenv("KAFKA_EVENTHUB_USERNAME")
	if user == "" {
		user = defaultUsername
	}

	consumerGroup := os.Getenv("KAFKA_EVENTHUB_CONSUMER_GROUP")
	if consumerGroup == "" {
		log.Fatal("please specify KAFKA_EVENTHUB_CONSUMER_GROUP environment variable")
	}
	log.Printf("KAFKA_EVENTHUB_CONSUMER_GROUP - %s\n", consumerGroup)

	topic := os.Getenv("KAFKA_EVENTHUB_TOPIC")
	if topic == "" {
		log.Fatal("please specify KAFKA_EVENTHUB_TOPIC environment variable")
	}
	log.Printf("KAFKA_EVENTHUB_TOPIC - %s\n", topic)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaEndpoint,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     user,
		"sasl.password":     eventHubConnectionString,
		"group.id":          consumerGroup,
		"auto.offset.reset": "earliest",
		//"debug":             "consumer",
	})

	if err != nil {
		log.Fatalf("unable to create consumer %v", err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("failed to subscribe to topic %v", err)
	}
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)
	var closed bool
	close := make(chan int)

	go func() {
		log.Println("waiting for messages...")
		for !closed {
			select {
			case <-exit:
				log.Println("shutdown request..")

				closed = true
				close <- 1
			default:
				msg, _ := consumer.ReadMessage(3 * time.Second)
				if msg != nil {
					log.Printf("message from kafka %s\n", string(msg.Value))
					time.Sleep(2 * time.Second)
				}
			}

		}

	}()

	log.Println("press ctrl+c to exit")
	<-close
	//consumer.Close()
	log.Println("exited...")

}
