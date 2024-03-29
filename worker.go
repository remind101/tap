package tap

import (
	"fmt"
	"log"
	"os"

	"code.google.com/p/go-uuid/uuid"

	"github.com/remind101/amqp"
	"github.com/remind101/metrics"
)

// ErrorHandler is an interface for draining errors to an exceptions aggregator.
type ErrorHandler interface {
	Handle(error) error
}

// nullErrorHandler is an ErrorHandler implementation that logs the error to Stderr.
type nullErrorHandler struct{}

// Handle implements the ErrorHandler Notify method.
func (h *nullErrorHandler) Handle(err error) error {
	_, err2 := fmt.Fprintf(os.Stderr, "%v\n", err)
	return err2
}

var DefaultErrorHandler = ErrorHandler(&nullErrorHandler{})

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

	// The body embedded in this message.
	Body []byte

	// The request id associated with this message.
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
	Logger       *log.Logger
	Queue        Queue
	Handler      Handler
	ErrorHandler ErrorHandler

	id string
}

type Options struct {
	ErrorHandler ErrorHandler
}

var DefaultOptions = &Options{
	ErrorHandler: DefaultErrorHandler,
}

// NewWorker returns a new Worker.
func NewWorker(q Queue, h Handler, o *Options) *Worker {
	if o == nil {
		o = DefaultOptions
	}

	id := uuid.New()
	l := log.New(os.Stdout, fmt.Sprintf("[worker] handler=%s id=%s ", h.Name(), id), 0)

	return &Worker{
		Logger:       l,
		Queue:        q,
		Handler:      h,
		ErrorHandler: o.ErrorHandler,
		id:           id,
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
			w.HandleMessage(newMessage(m))
			t.Done()
		case <-shutdown:
			w.Logger.Printf("at=close sigterm received, attempting to shut down gracefully\n")
			if err := w.Queue.Close(); err != nil {
				w.HandleError(err)
			}

			if err := w.Handler.Stop(); err != nil {
				w.HandleError(err)
			}

			return
		}
	}
}

func (w *Worker) HandleMessage(m *Message) {
	w.Handler.Handle(m)
}

func (w *Worker) HandleError(err error) error {
	return w.ErrorHandler.Handle(err)
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
