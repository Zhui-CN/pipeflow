package rmqEndpoint

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// Conf rmq connection configuration
type Conf struct {
	Host     string
	Port     int
	User     string
	Password string
}

// DefaultConf test default rmq connection configuration
func DefaultConf() Conf {
	return Conf{
		Host:     "127.0.0.1",
		Port:     5672,
		User:     "admin",
		Password: "147963",
	}
}

type rmqClient struct {
	channel *amqp.Channel
}

func (c *rmqClient) queueDeclare(queue string) {
	if _, err := c.channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		log.Println("Error: rmq QueueDeclare err", err.Error())
	}
}

func newRmq(conf Conf) *rmqClient {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", conf.User, conf.Password, conf.Host, conf.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalln("cannot dial amqp:", err.Error())
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("cannot allocate channel:", err.Error())
	}
	return &rmqClient{channel: ch}
}
