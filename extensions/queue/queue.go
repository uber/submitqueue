package queue

// Queue creates and manages queue publishers and subscribers.
// Implementations handle connection pooling, consumer group configuration,
// and resource lifecycle.
type Queue interface {
	// Publisher returns a Publisher instance.
	// May return a singleton or new instance depending on implementation.
	Publisher() Publisher

	// Subscriber returns a Subscriber instance.
	// May return a singleton or new instance depending on implementation.
	Subscriber() Subscriber

	// Close shuts down the queue and all associated resources.
	Close() error
}
