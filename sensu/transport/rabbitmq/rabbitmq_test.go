package rabbitmq

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/upfluence/sensu-go/Godeps/_workspace/src/github.com/streadway/amqp"
)

var (
	dummyAmqpChannel = &mockAmqpChannel{}
)

type mockAmqpChannel struct {
}

func (*mockAmqpChannel) Consume(
	string,
	string,
	bool,
	bool,
	bool,
	bool,
	amqp.Table,
) (<-chan amqp.Delivery, error) {
	return nil, nil
}
func (*mockAmqpChannel) ExchangeDeclare(
	string,
	string,
	bool,
	bool,
	bool,
	bool,
	amqp.Table,
) error {
	return nil
}
func (*mockAmqpChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	// We need to close the channel here, in order to prevent the goroutine at
	// the end of transport.Connect() from blocking indefinitely.
	close(c)
	return nil
}
func (*mockAmqpChannel) Publish(string, string, bool, bool, amqp.Publishing) error {
	return nil
}
func (*mockAmqpChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	return nil
}
func (*mockAmqpChannel) QueueDeclare(
	string,
	bool,
	bool,
	bool,
	bool,
	amqp.Table,
) (amqp.Queue, error) {
	return amqp.Queue{}, nil
}

type mockAmqpConnection struct {
	heartbeat time.Duration
}

func (*mockAmqpConnection) Channel() (amqpChannel, error) {
	return dummyAmqpChannel, nil
}

func (*mockAmqpConnection) Close() error {
	return nil
}

func mockAmqpDialer(url string) (amqpConnection, error) {
	return &mockAmqpConnection{}, nil
}

func mockAmqpDialerConfig(url string, config amqp.Config) (amqpConnection, error) {
	return &mockAmqpConnection{heartbeat: config.Heartbeat}, nil
}

func getDummyTransportConfig(heartbeat int) *TransportConfig {
	config, _ := NewTransportConfig("amqp://guest:guest@localhost:5672/%2F")

	if heartbeat == 0 {
		return config
	}

	config.Heartbeat = json.Number(strconv.Itoa(heartbeat))
	return config
}

var transportConnectTestScenarios = []struct {
	config            *TransportConfig
	expectedHeartbeat time.Duration
}{
	{
		getDummyTransportConfig(0),
		time.Duration(0),
	},
	{
		getDummyTransportConfig(41),
		time.Duration(41 * time.Second),
	},
}

func TestTransportConnect(t *testing.T) {
	for _, scenario := range transportConnectTestScenarios {
		transport := &RabbitMQTransport{
			ClosingChannel: make(chan bool),
			Configs:        []*TransportConfig{scenario.config},
			dialer:         mockAmqpDialer,
			dialerConfig:   mockAmqpDialerConfig,
		}

		err := transport.Connect()

		validateError(err, nil, t)

		heartbeat := transport.Connection.(*mockAmqpConnection).heartbeat
		if heartbeat != scenario.expectedHeartbeat {
			t.Errorf("Expected heartbeat to be \"%s\" but got \"%s\" instead",
				scenario.expectedHeartbeat,
				heartbeat,
			)
		}

		if transport.Channel != dummyAmqpChannel {
			t.Error("Expected transport.Channel to be set to dummyAmqpChannel")
		}

		channelClosed := <-transport.ClosingChannel

		if !channelClosed {
			t.Error("Failed to close channel")
		}

		err = transport.Close()

		if err != nil {
			t.Fatalf("Expected error to be nil but got \"%s\" instead", err)
		}

		if transport.Channel != nil {
			t.Error(
				"Expected channel to be nil, but got \"%+v\" instead",
				transport.Channel,
			)
		}

		if transport.Connection != nil {
			t.Error(
				"Expected connection to be nil, but got \"%+v\" instead",
				transport.Connection,
			)
		}
	}
}

var dummyDialerError = errors.New("Dummy dialer error")

func mockAmqpDialerError(url string) (amqpConnection, error) {
	return nil, dummyDialerError
}

func TestTransportConnectError(t *testing.T) {
	transport := &RabbitMQTransport{
		ClosingChannel: make(chan bool),
		Configs:        []*TransportConfig{getDummyTransportConfig(0)},
		dialer:         mockAmqpDialerError,
		dialerConfig:   mockAmqpDialerConfig,
	}

	err := transport.Connect()

	validateError(err, dummyDialerError, t)
}
