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
	dummyAMQPChannel = &mockAMQPChannel{}
)

type mockAMQPChannel struct {
}

func (*mockAMQPChannel) Consume(
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

func (*mockAMQPChannel) ExchangeDeclare(
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

func (*mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	// We need to close the channel here, in order to prevent the goroutine at
	// the end of transport.Connect() from blocking indefinitely.
	close(c)
	return nil
}

func (*mockAMQPChannel) Publish(string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func (*mockAMQPChannel) Qos(int, int, bool) error {
	return nil
}

func (*mockAMQPChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	return nil
}

func (*mockAMQPChannel) QueueDeclare(
	string,
	bool,
	bool,
	bool,
	bool,
	amqp.Table,
) (amqp.Queue, error) {
	return amqp.Queue{}, nil
}

type mockAMQPConnection struct {
	heartbeat time.Duration
}

func (*mockAMQPConnection) Channel() (AMQPChannel, error) {
	return dummyAMQPChannel, nil
}

func (*mockAMQPConnection) Close() error {
	return nil
}

func mockAMQPDialer(url string) (AMQPConnection, error) {
	return &mockAMQPConnection{}, nil
}

func mockAMQPDialerConfig(url string, config amqp.Config) (AMQPConnection, error) {
	return &mockAMQPConnection{heartbeat: config.Heartbeat}, nil
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
			dialer:         mockAMQPDialer,
			dialerConfig:   mockAMQPDialerConfig,
		}

		err := transport.Connect()

		validateError(err, nil, t)

		heartbeat := transport.Connection.(*mockAMQPConnection).heartbeat
		if heartbeat != scenario.expectedHeartbeat {
			t.Errorf("Expected heartbeat to be \"%s\" but got \"%s\" instead",
				scenario.expectedHeartbeat,
				heartbeat,
			)
		}

		if transport.Channel != dummyAMQPChannel {
			t.Error("Expected transport.Channel to be set to dummyAMQPChannel")
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
			t.Errorf(
				"Expected channel to be nil, but got \"%+v\" instead",
				transport.Channel,
			)
		}

		if transport.Connection != nil {
			t.Errorf(
				"Expected connection to be nil, but got \"%+v\" instead",
				transport.Connection,
			)
		}
	}
}

var errFailedToConnect = errors.New("Dummy dialer error")

func mockAMQPDialerError(url string) (AMQPConnection, error) {
	return nil, errFailedToConnect
}

func TestTransportConnectError(t *testing.T) {
	transport := &RabbitMQTransport{
		ClosingChannel: make(chan bool),
		Configs:        []*TransportConfig{getDummyTransportConfig(0)},
		dialer:         mockAMQPDialerError,
	}

	err := transport.Connect()

	validateError(err, errFailedToConnect, t)
}
