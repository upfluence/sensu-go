package rabbitmq

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/upfluence/sensu-go/Godeps/_workspace/src/github.com/streadway/amqp"
	"github.com/upfluence/sensu-go/Godeps/_workspace/src/github.com/upfluence/goutils/log"
)

// AmqpChannel is an interface over amqp.Channel
type AmqpChannel interface {
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Publish(string, string, bool, bool, amqp.Publishing) error
	QueueBind(string, string, string, bool, amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
}

// AmqpConnection is an interface over amqp.Connection
type AmqpConnection interface {
	Channel() (AmqpChannel, error)
	Close() error
}

// Connection is a wrapper for amqp.Connection needed to be able
// to assign the result of the dialer function to
// RabbitMQTransport.Connection, because Go doesn't support
// covariant return types
// Relevant discussion: https://github.com/streadway/amqp/issues/164
type Connection struct {
	*amqp.Connection
}

// Channel is a wrapper for amqp.Connection.Channel()
func (c *Connection) Channel() (AmqpChannel, error) {
	return c.Connection.Channel()
}

// Close is a wrapper for amqp.Connection.Close()
func (c *Connection) Close() error {
	return c.Connection.Close()
}

// RabbitMQTransport contains AMQP objects required to communicate with RabbitMQ
type RabbitMQTransport struct {
	Connection     AmqpConnection
	Channel        AmqpChannel
	ClosingChannel chan bool
	Configs        []*TransportConfig
	dialer         func(string) (AmqpConnection, error)
	dialerConfig   func(string, amqp.Config) (AmqpConnection, error)
}

// NewRabbitMQTransport creates a RabbitMQTransport instance from a given URI
func NewRabbitMQTransport(uri string) (*RabbitMQTransport, error) {
	config, err := NewTransportConfig(uri)

	if err != nil {
		return nil, fmt.Errorf("Received invalid URI: %s", err)
	}

	return NewRabbitMQHATransport([]*TransportConfig{config}), nil
}

func amqpDialer(url string) (AmqpConnection, error) {
	var conn = &Connection{}
	var err error
	conn.Connection, err = amqp.Dial(url)

	return conn, err
}

func amqpDialerConfig(url string, config amqp.Config) (AmqpConnection, error) {
	var conn = &Connection{}
	var err error
	conn.Connection, err = amqp.DialConfig(url, config)

	return conn, err
}

// NewRabbitMQHATransport creates a RabbitMQTransport instance from a list of
// TransportConfig objects in order to connect to a
// High Availability RabbitMQ cluster
func NewRabbitMQHATransport(configs []*TransportConfig) *RabbitMQTransport {
	return &RabbitMQTransport{
		ClosingChannel: make(chan bool),
		Configs:        configs,
		dialer:         amqpDialer,
		dialerConfig:   amqpDialerConfig,
	}
}

func (t *RabbitMQTransport) GetClosingChan() chan bool {
	return t.ClosingChannel
}

func (t *RabbitMQTransport) Connect() error {
	var (
		uri           string
		err           error
		randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	)

	for _, idx := range randGenerator.Perm(len(t.Configs)) {
		config := t.Configs[idx]
		uri = config.GetURI()

		log.Noticef("Trying to connect to URI: %s", uri)

		// TODO: Figure out how to specify the Prefetch value as well
		// See amqp.Channel.Qos (it doesn't seem to be used currently)

		// TODO: Add SSL support via amqp.DialTLS

		if heartbeatString := config.Heartbeat.String(); heartbeatString != "" {
			var heartbeat time.Duration
			heartbeat, err = time.ParseDuration(heartbeatString + "s")

			if err != nil {
				log.Warningf("Failed to parse the heartbeat: %s", uri, err.Error())
				continue
			}

			t.Connection, err = t.dialerConfig(
				uri,
				amqp.Config{Heartbeat: heartbeat},
			)
		} else {
			t.Connection, err = t.dialer(uri)
		}

		if err != nil {
			log.Warningf("Failed to connect to URI %s: %s", uri, err.Error())
			continue
		}

		break
	}

	if err != nil {
		log.Errorf("RabbitMQ connection error: %s", err.Error())
		return err
	}

	t.Channel, err = t.Connection.Channel()

	if err != nil {
		log.Errorf("RabbitMQ channel error: %s", err.Error())
		return err
	}

	log.Noticef("RabbitMQ connection and channel opened to %s", uri)

	closeChan := make(chan *amqp.Error)
	t.Channel.NotifyClose(closeChan)

	go func() {
		<-closeChan
		t.ClosingChannel <- true
	}()

	return nil
}

func (t *RabbitMQTransport) IsConnected() bool {
	if t.Connection == nil || t.Channel == nil {
		return false
	}

	return true
}

func (t *RabbitMQTransport) Close() error {
	if t.Connection == nil {
		return errors.New("The connection is not opened")
	}

	defer func() {
		t.Channel = nil
		t.Connection = nil
	}()
	t.Connection.Close()

	return nil
}

func (t *RabbitMQTransport) Publish(exchangeType, exchangeName, key string, message []byte) error {
	if t.Channel == nil {
		return errors.New("The channel is not opened")
	}

	if err := t.Channel.ExchangeDeclare(exchangeName, exchangeType, false, false, false, false, nil); err != nil {
		return err
	}

	err := t.Channel.Publish(exchangeName, key, false, false, amqp.Publishing{Body: message})

	return err
}

func (t *RabbitMQTransport) Subscribe(key, exchangeName, queueName string, messageChan chan []byte, stopChan chan bool) error {
	if t.Channel == nil {
		return errors.New("The channel is not opened")
	}

	if err := t.Channel.ExchangeDeclare(
		exchangeName,
		"fanout",
		false,
		false,
		false,
		false,
		amqp.Table{},
	); err != nil {
		log.Errorf("Can't declare the exchange: %s", err.Error())
		return err
	}

	log.Infof("Exchange %s declared", exchangeName)

	if _, err := t.Channel.QueueDeclare(
		queueName,
		false,
		true,
		false,
		false,
		nil,
	); err != nil {
		log.Errorf("Can't declare the queue: %s", err.Error())
		return err
	}

	log.Infof("Queue %s declared", queueName)

	if err := t.Channel.QueueBind(queueName, key, exchangeName, false, nil); err != nil {
		log.Errorf("Can't bind the queue: %s", err.Error())
		return err
	}

	log.Noticef("Queue %s binded to %s for key %s", queueName, exchangeName, key)

	deliveryChange, err := t.Channel.Consume(queueName, "", true, false, false, false, nil)

	log.Infof("Consuming the queue %s", queueName)

	if err != nil {
		log.Errorf("Can't consume the queue: %s", err.Error())
		return err
	}

	for {
		select {
		case delivery, ok := <-deliveryChange:
			if ok {
				messageChan <- delivery.Body
			} else {
				t.ClosingChannel <- true
				break
			}
		case <-stopChan:
			break
		}
	}
}
