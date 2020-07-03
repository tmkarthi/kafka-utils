package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"

	"io"

	"crypto/tls"

	"github.com/bsm/sarama-cluster"
	"github.com/jessevdk/go-flags"
)

type Settings struct {
	ServerHost    string `short:"s" description:"Kafka server host" default:"localhost:9092"`
	Topic         string `short:"t" description:"Topic to read from" default:"sample"`
	OutputFile    string `short:"o" description:"Output file. Specify '-' for printing to the console" default:"-"`
	ConsumerGroup string `short:"c" description:"ConsumerGroup name" default:"simple-consumer"`
	UseSSL        bool   `long:"use-ssl" description:"Use SSL"`
}

func main() {
	sarama.Logger = logrus.StandardLogger().WithFields(logrus.Fields{"P": "ARM", "L": "Sarama"})
	settings := &Settings{}
	_, err := flags.Parse(settings)
	if err != nil {
		if err := err.(*flags.Error); err.Type == flags.ErrHelp {
			return
		}
		panic(err)
	}
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	if settings.UseSSL {
		config.Net.TLS.Enable = true
		tlsConfig := tls.Config{InsecureSkipVerify: true}
		config.Net.TLS.Config = &tlsConfig
	}

	// init consumer
	brokers := []string{settings.ServerHost}
	topics := []string{settings.Topic}
	consumer, err := cluster.NewConsumer(brokers, settings.ConsumerGroup, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	var f io.Writer
	if settings.OutputFile == "-" {
		f = os.Stdout
	} else {
		f, err := os.OpenFile(settings.OutputFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		defer f.Close()
	}

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				fmt.Fprintf(f, "%s\n", msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case err, more := <-consumer.Errors():
			if more {
				os.Stderr.Write([]byte(fmt.Sprintf("Error: %s\n", err.Error())))
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				os.Stderr.Write([]byte(fmt.Sprintf("Rebalanced: %v\n", ntf)))
			}
		case <-signals:
			return
		}
	}
}
