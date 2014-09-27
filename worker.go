package tap

import (
	"fmt"
	"log"
	"os"

	"code.google.com/p/go-uuid/uuid"

	"github.com/remind101/amqp"
	"github.com/remind101/metrics"
)

var (
	// Errors is called when there are unhandled errors.
	Errors = ErrorHandler(&NullErrorHandler{})
)

// Handler is an interface that can be implemented for handling messages for
// processing.
type Handler interface {
	Name() string
	Handle(*Message)
	Stop() error
}

// Queue hides the details of rabbitmq behind a simple interface.
type Queue interface {
	// Name returns the name of the queue.
	Name() string

	// Subscribe starts pushing messages to the channel.
	Subscribe(messages chan<- *amqp.Message) error

	// Close is called when the queue should stop consuming messages and close.
	Close() error
}

// Message represents a message that is popped off of the queue.
type Message struct {
	amqp.Acknowledger

	// The event embedded in this message.
	Body []byte

	// The request id associated with this event.
	RequestID string
}

// Acknowledger wraps an amqp.Acknowledger with instrumentation.
type Acknowledger struct {
	amqp.Acknowledger
}

// Ack acknowledges the message.
func (a *Acknowledger) Ack() error {
	t := metrics.Time("message.ack")
	defer t.Done()

	return a.Acknowledger.Ack()
}

// When we Nack a message, we either drop it or requeue it. This maps the requeue paramater
// of Nack to a metric name.
var nackMetrics = map[bool]string{
	true:  "requeued",
	false: "dropped",
}

// Nack negatively acknowledges the message.
func (a *Acknowledger) Nack(requeue bool) error {
	t := metrics.Time(fmt.Sprintf("message.%s", nackMetrics[requeue]))
	defer t.Done()

	return a.Acknowledger.Nack(requeue)
}

// Worker consumes messages off of the queue and hands them off to a handler.
type Worker struct {
	Logger  *log.Logger
	Queue   Queue
	Handler Handler

	id string
}

// NewWorker returns a new Worker.
func NewWorker(q Queue, h Handler) *Worker {
	id := uuid.New()
	l := log.New(os.Stdout, fmt.Sprintf("[worker] handler=%s id=%s ", h.Name(), id), 0)

	return &Worker{
		Logger:  l,
		Queue:   q,
		Handler: h,
		id:      id,
	}
}

// Start starts the worker.
func (w *Worker) Start(shutdown chan interface{}) {
	mm := make(chan *amqp.Message)
	w.Queue.Subscribe(mm)

	w.Logger.Printf("at=started queue=%s\n", w.Queue.Name())

	for {
		select {
		case m := <-mm:
			t := metrics.Time("message.handle")
			w.Handler.Handle(newMessage(m))
			t.Done()
		case <-shutdown:
			w.Logger.Printf("at=close sigterm received, attempting to shut down gracefully\n")
			if err := w.Queue.Close(); err != nil {
				NotifyError(err)
			}

			if err := w.Handler.Stop(); err != nil {
				NotifyError(err)
			}

			return
		}
	}
}

func newMessage(m *amqp.Message) *Message {
	requestID := ""
	if m.Headers["request_id"] != nil {
		if rid, ok := m.Headers["request_id"].(string); ok {
			requestID = rid
		}
	}

	return &Message{
		Acknowledger: &Acknowledger{m.Acknowledger},
		Body:         m.Body,
		RequestID:    requestID,
	}
}

// NotifyError can be called by handlers to drain an error to the error handler.
func NotifyError(v interface{}) error {
	return Errors.Notify(v)
}
