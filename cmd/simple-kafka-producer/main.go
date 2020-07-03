package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	flags "github.com/jessevdk/go-flags"
)

type Settings struct {
	ServerHost  string `short:"s" description:"Kafka server host" default:"localhost:9092"`
	Topic       string `short:"t" description:"Topic to publish to" default:"sample"`
	Key         string `short:"k" description:"Message Key. If -m is set as +, this should be either '+' for reading from stdin or key value for sending same key for all messages."`
	MessageFile string `short:"m" description:"File containing message value. If specified as '-', will read from stdin till an empty line is encountered. If specified as '+', every line in stdin will be published as one message till killed through ctrl+c. If value of '+' is accompanied by -k with 'true', every odd numbered lines will be considered as key." required:"true"`
}

func main() {
	settings := &Settings{}
	_, err := flags.Parse(settings)
	if err != nil {
		if err := err.(*flags.Error); err.Type == flags.ErrHelp {
			return
		}
		panic(err)
	}
	producer, err := sarama.NewSyncProducer([]string{settings.ServerHost}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	if settings.MessageFile == "+" {
		s := bufio.NewScanner(os.Stdin)
		var key []byte
		if string(settings.Key) != "+" {
			key = []byte(settings.Key)
		}
		for s.Scan() {
			if key == nil {
				key = s.Bytes()
			} else {
				contents := s.Bytes()
				publish(settings.Topic, key, contents, producer)
				if string(settings.Key) == "+" {
					key = nil
				}
			}
		}
	} else {
		var contents []byte
		if settings.MessageFile == "-" {
			contents, err = readFromStdin()
		} else {
			contents, err = ioutil.ReadFile(settings.MessageFile)
		}
		if err != nil {
			panic(err)
		}
		publish(settings.Topic, []byte(settings.Key), contents, producer)
	}
}

func publish(topic string, key []byte, contents []byte, producer sarama.SyncProducer) {
	msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(contents)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}

func readFromStdin() ([]byte, error) {
	var contents []byte
	first := true
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		bytes := s.Bytes()
		if len(bytes) == 0 {
			return contents, nil
		}
		if first {
			first = false
		} else {
			contents = append(contents, '\n')
		}
		contents = append(contents, bytes...)
	}
	return contents, s.Err()
}
