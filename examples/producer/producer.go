package main

import (
	"context"
	"log"

	amqp "github.com/fogcloud-io/fog-amqp"
)

var (
	AMQPHost        = "localhost"
	AMQPPort        = "5672"
	AMQPTLS         = false
	FogAccessKey    = "xgHc40bf04fb020c"
	FogAccessSecret = "c3bad348bb34390558f7f1aacce17877"
	clientID        = "fog-producer"
)

func main() {
	cli, err := amqp.NewFogAMQPClient(AMQPHost, AMQPPort, FogAccessKey, FogAccessSecret, clientID, AMQPTLS)
	if err != nil {
		log.Fatal(err)
	}

	err = cli.PublishUnsafeWithContext(
		context.Background(),
		"test",
		"test",
		[]byte("hello,world"),
		amqp.WithPublishOptionsContentType("text/plaintext"),
		amqp.WithPublishOptionsDeliveryMode(2),
	)
	if err != nil {
		log.Print(err)
	}
}
