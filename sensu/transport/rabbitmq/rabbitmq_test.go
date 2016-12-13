package rabbitmq

import (
	"errors"
	"testing"
)

func TestTransportConnect(t *testing.T) {
	testConfig, _ := NewTransportConfig("amqp://guest:guest@localhost:5672/%2F")

	transport := NewRabbitMQHATransport([]*TransportConfig{testConfig})

	err := transport.Connect()

	validateError(err, errors.New("dial tcp [::1]:5672: getsockopt: connection refused"), t)
}
