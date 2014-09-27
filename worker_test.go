package tap

import (
	"testing"

	"github.com/remind101/amqp"
)

func Test_NewMessageFromDelivery(t *testing.T) {
	tests := []struct {
		in  amqp.Message
		out Message
	}{
		// Delivery with nil body and nil request id.
		{
			amqp.Message{
				Body: nil,
			},
			Message{
				Body: nil,
			},
		},

		// Delivery with a request id.
		{
			amqp.Message{
				Headers: map[string]interface{}{
					"request_id": "123456789",
				},
			},
			Message{
				RequestID: "123456789",
			},
		},

		// Delivery with an integer request id.
		{
			amqp.Message{
				Headers: map[string]interface{}{
					"request_id": 1234,
				},
			},
			Message{
				RequestID: "",
			},
		},
	}

	for _, tt := range tests {
		m := newMessage(&tt.in)

		if m.RequestID != tt.out.RequestID {
			t.Errorf("Message.RequestID => %q; want %q", m.RequestID, tt.out.RequestID)
		}
	}
}
