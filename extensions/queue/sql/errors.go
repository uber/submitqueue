package sql

import "fmt"

// ErrAlreadyAcknowledged is returned when attempting to ack/nack a delivery that was already processed
type ErrAlreadyAcknowledged struct {
	DeliveryID string
}

func (e *ErrAlreadyAcknowledged) Error() string {
	return fmt.Sprintf("delivery %s already acknowledged or nacked", e.DeliveryID)
}
