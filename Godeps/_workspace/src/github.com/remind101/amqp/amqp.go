package amqp

import (
	"errors"

	"github.com/streadway/amqp"
)

var (
	// DefaultURL is the default amqp url to connect to.
	DefaultURL = "amqp://localhost"

	// DefaultExchangeOptions are the default options used when building a new Exchange.
	DefaultExchangeOptions = &ExchangeOptions{
		Name:       "hutch",
		Type:       "topic",
		Durable:    true,
		AutoDelete: false,
	}

	// DefaultQueueOptions are the default options used when building a new Queue.
	DefaultQueueOptions = &QueueOptions{
		Durable:    true,
		AutoDelete: false,
		RoutingKey: "",
	}
)

// ExchangeOptions can be passed to NewExchange to configure the Exchange.
type ExchangeOptions struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// QueueOptions can be passed to NewQueue to configure the queue.
type QueueOptions struct {
	Durable    bool
	AutoDelete bool
	RoutingKey string
}

// Exchange represents an amqp exchange and wraps an amqp.Connection
// and an amqp.Channel.
type Exchange struct {
	Name       string
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewExchange connects to rabbitmq, opens a channel and returns a new
// Exchange instance. If url is an empty string, it will attempt to connect
// to localhost.
func NewExchange(url string, options *ExchangeOptions) (*Exchange, error) {
	if url == "" {
		url = DefaultURL
	}

	if options == nil {
		options = DefaultExchangeOptions
	}

	c, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		options.Name,       // name
		options.Type,       // kind
		options.Durable,    // durable
		options.AutoDelete, // autoDelete
		false,              // internal
		false,              // noWait
		nil,                // args
	)
	if err != nil {
		return nil, err
	}

	return &Exchange{
		Name:       options.Name,
		connection: c,
		channel:    ch,
	}, nil
}

// Publish publishes a message to the Exchange.
func (e *Exchange) Publish(routingKey, message, requestID string) error {
	msg := amqp.Publishing{
		Headers: amqp.Table{
			"request_id": requestID,
		},
		ContentType:  "application/json",
		Body:         []byte(message),
		DeliveryMode: amqp.Persistent,
		Priority:     0,
	}

	return e.channel.Publish(
		e.Name,     // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // imediate
		msg,        // message
	)
}

// Close closes the connection.
func (e *Exchange) Close() error {
	if err := e.channel.Close(); err != nil {
		return err
	}

	return e.connection.Close()
}

// Queue represents an amqp queue.
type Queue struct {
	exchange   *Exchange
	routingKey string
	name       string
}

// NewQueue returns a new Queue instance.
func NewQueue(queue string, exchange *Exchange, options *QueueOptions) (*Queue, error) {
	if options == nil {
		options = DefaultQueueOptions
	}

	_, err := exchange.channel.QueueDeclare(
		queue,              // name
		options.Durable,    // durable
		options.AutoDelete, // autoDelete
		false,              // exclusive
		false,              // noWait
		nil,                // args
	)
	if err != nil {
		return nil, err
	}

	return &Queue{
		exchange:   exchange,
		routingKey: options.RoutingKey,
		name:       queue,
	}, nil
}

// Purge purges all messages in the queue.
func (q *Queue) Purge() error {
	_, err := q.exchange.channel.QueuePurge(q.name, false)
	return err
}

// Name returns the name of the queue.
func (q *Queue) Name() string {
	return q.name
}

// Subscribe starts consuming from the queue.
func (q *Queue) Subscribe(messages chan<- *Message) error {
	if err := q.bind(); err != nil {
		return err
	}

	dd, err := q.exchange.channel.Consume(
		q.name,           // queue
		q.consumerName(), // consumer name
		false,            // autoAck
		false,            // exclusive
		false,            // noLocal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case d := <-dd:
				m := &Message{
					Acknowledger: &acknowledger{
						Acknowledger: d.Acknowledger,
						deliveryTag:  d.DeliveryTag,
					},
					Headers: d.Headers,
					Body:    d.Body,
				}

				messages <- m
			}
		}
	}()

	return nil
}

// Close closes the exchange.
func (q *Queue) Close() error {
	if err := q.exchange.channel.Cancel(q.consumerName(), false); err != nil {
		return err
	}

	ch := q.exchange.channel.NotifyClose(make(chan *amqp.Error))
	q.exchange.Close()

	// Wait for the deliveries to drain.
	if err := <-ch; err != nil {
		return err
	}

	return nil
}

// bind binds the queue. This is called automatically when Subscribe is called.
func (q *Queue) bind() error {
	return q.exchange.channel.QueueBind(
		q.name,          // name
		q.routingKey,    // key
		q.exchange.Name, // exchange
		false,           // noWait
		nil,             // args
	)
}

func (q *Queue) consumerName() string {
	return q.name
}

// Message represents an amqp message.
type Message struct {
	Acknowledger
	Headers map[string]interface{}
	Body    []byte
}

// Acknowledger allows a message to be acked or nacked (rejected).
type Acknowledger interface {
	Ack() error
	Nack(requeue bool) error
}

// acknowledger wraps an amqp.Acknowledger to implement the Acknowledger interface.
type acknowledger struct {
	amqp.Acknowledger
	deliveryTag uint64
}

// Ack implements Acknowledger Ack.
func (d *acknowledger) Ack() error {
	return d.Acknowledger.Ack(d.deliveryTag, false)
}

// Nack implements Acknowledger Nack.
func (d *acknowledger) Nack(requeue bool) error {
	return d.Acknowledger.Nack(d.deliveryTag, false, requeue)
}

// Acknowledgement specifieds an acknowledgement type.
type Acknowledgement int

func (a Acknowledgement) String() string {
	switch a {
	case Acked:
		return "acked"
	case Requeued:
		return "requeued"
	case Dropped:
		return "dropped"
	}

	return "unacknowledged"
}

// Used with the NullAcknowledger to determine the Acknowledgement type.
const (
	Unacknowledged Acknowledgement = iota
	Acked
	Requeued
	Dropped
)

// ErrAlreadyAcked is returned by the NullAcknowledger if the message has already been
// acked.
var ErrAlreadyAcked = errors.New("already acked")

// NullAcknowledger is an implementation of the amqp.Acknowledger interface that
// stores the acknowledgement in a variable.
type NullAcknowledger struct {
	acked           bool
	Acknowledgement Acknowledgement
}

// Ack sets acked to true.
func (a *NullAcknowledger) Ack() error {
	if a.acked {
		return ErrAlreadyAcked
	}

	a.acked = true
	a.Acknowledgement = Acked
	return nil
}

// Nack sets nacked to true.
func (a *NullAcknowledger) Nack(requeue bool) error {
	if a.acked {
		return ErrAlreadyAcked
	}

	a.acked = true
	if requeue {
		a.Acknowledgement = Requeued
	} else {
		a.Acknowledgement = Dropped
	}
	return nil
}
