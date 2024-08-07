# fog-amqp
[![standard-readme compliant](https://img.shields.io/badge/licence-Apache%202.0-blue)](https://www.apache.org/licenses/LICENSE-2.0) [![standard-readme compliant](https://img.shields.io/static/v1?label=official&message=demo&color=<COLOR>)](https://app.fogcloud.io)

中文 | [English](readme.en.md)

FogCloud AMQP SDK。 基于开源库[amqp091-go](https://github.com/rabbitmq/amqp091-go)开发，增加了重连逻辑和更易用的API。

## 安装

```bash
go get github.com/fogcloud-io/fog-amqp@latest
```

## 快速上手

### 消费者
```golang
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
```

### 生产者
```golang
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
```
