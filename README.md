# kafka-utils
Utils for working with Kafka

# Installation
To install, run

    go get github.com/tmkarthi/kafka-utils/...

# simple-kafka-producer
```
Usage:
  simple-kafka-producer [OPTIONS]

Application Options:
  -s=         Kafka server host (default: localhost:9092)
  -t=         Topic to publish to (default: sample)
  -k=         Message Key. If -m is set as +, this should be either '+' for reading from stdin or
              key value for sending same key for all messages.
  -m=         File containing message value. If specified as '-', will read from stdin till an empty
              line is encountered. If specified as '+', every line in stdin will be published as one
              message till killed through ctrl+c. If value of '+' is accompanied by -k with '+',
              every odd numbered lines will be considered as key.

Help Options:
  -h, --help  Show this help message
```
# simple-kafka-consumer
```
Usage:
  simple-kafka-consumer [OPTIONS]

Application Options:
  -s=            Kafka server host (default: localhost:9092)
  -t=            Topic to read from (default: sample)
  -o=            Output file. Specify '-' for printing to the console (default: -)
  -c=            ConsumerGroup name (default: simple-consumer)
      --use-ssl  Use SSL

Help Options:
  -h, --help     Show this help message
```
