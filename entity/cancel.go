package entity

import "encoding/json"

// Cancel represents a request to cancel a previously submitted land request.
// The object is immutable after creation.
type Cancel struct {
	// Sqid is the globally unique identifier of the land request to cancel.
	Sqid string `json:"sqid"`
}

// ToBytes serializes the Cancel to JSON bytes for queue message payload.
func (c Cancel) ToBytes() ([]byte, error) {
	return json.Marshal(c)
}

// CancelFromBytes deserializes a Cancel from JSON bytes.
func CancelFromBytes(data []byte) (Cancel, error) {
	var c Cancel
	err := json.Unmarshal(data, &c)
	return c, err
}
