package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/upfluence/goutils/log"
)

type RabbitMQTransportConfig struct {
	Host      string      `json:"host,omitempty"`
	Port      json.Number `json:"port,omitempty"`
	Vhost     string      `json:"vhost,omitempty"`
	User      string      `json:"user,omitempty"`
	Password  string      `json:"password,omitempty"`
	Heartbeat json.Number `json:"heartbeat,omitempty"`
	Prefetch  json.Number `json:"prefetch,omitempty"`
	Ssl       struct {
		CertChainFile  string `json:"cert_chain_file,omitempty"`
		PrivateKeyFile string `json:"private_key_file,omitempty"`
	} `json:"ssl,omitempty"`

	uri string `json:"-"`
}

func (c *RabbitMQTransportConfig) GetURI() string {
	if c.uri == "" {
		c.uri = fmt.Sprintf(
			"amqp://%s:%s@%s:%s/%s",
			c.User,
			c.Password,
			c.Host,
			c.Port.String(),
			url.QueryEscape(c.Vhost),
		)
	}

	return c.uri
}

func NewRabbitMQTransportConfig(uri string) (*RabbitMQTransportConfig, error) {
	uriComponents, err := url.Parse(uri)
	if err != nil {
		log.Errorf("Failed to parse the URI: %s", err)
		return nil, err
	}

	if !strings.Contains(uriComponents.Host, ":") {
		message := fmt.Sprintf("Failed to determine the port for host: %s", uriComponents.Host)
		log.Error(message)
		return nil, errors.New(message)
	}

	host, port, err := net.SplitHostPort(uriComponents.Host)
	if err != nil {
		log.Errorf("Failed to separate the host name from the port: %s", err)
		return nil, err
	}

	user := uriComponents.User.Username()
	password, _ := uriComponents.User.Password()

	return &RabbitMQTransportConfig{
		Host:     host,
		Port:     json.Number(port),
		Vhost:    uriComponents.Path[1:], // Discard the leading slash
		User:     user,
		Password: password,
	}, nil
}
