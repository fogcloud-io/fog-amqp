package main

import (
	"log"

	amqp "github.com/fogcloud-io/fog-amqp"
)

var (
	AMQPHost        = "localhost"
	AMQPPort        = "5672"
	AMQPTLS         = false
	FogAccessKey    = "xgHc40bf04fb020c"
	FogAccessSecret = "c3bad348bb34390558f7f1aacce17877"
	clientID        = "fog-consumer"
)

func main() {
	cli, err := amqp.NewFogAMQPClient(AMQPHost, AMQPPort, FogAccessKey, FogAccessSecret, clientID, AMQPTLS)
	if err != nil {
		log.Fatal(err)
	}

	err = cli.ConsumeWithHandler(
		100,
		FogAccessKey,
		func(b amqp.Delivery) { log.Printf("amqp receive: %s", b.Body) },
		amqp.WithConsumerOptionsConsumerTag("fog-consumer-1"),
		amqp.WithConsumerOptionsAutoAck(true),
		amqp.WithConsumerOptionsNoWait(true),
	)
	if err != nil {
		log.Print(err)
	}
}
