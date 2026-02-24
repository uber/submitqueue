package consumer

import "github.com/uber/submitqueue/extension/queue"

// Topic identifies a queue topic in the pipeline.
type Topic string

const (
	// TopicRequest is where new requests arrive from the gateway.
	TopicRequest Topic = "request"
	// TopicToBatch is where validated requests are published for batching.
	TopicToBatch Topic = "to-batch"
)

// String returns the topic name as a string.
func (t Topic) String() string {
	return string(t)
}

// TopicConfig maps a topic to its queue backend.
type TopicConfig struct {
	// Topic is the topic identifier.
	Topic Topic
	// Queue is the queue backend for this topic.
	Queue queue.Queue
}

// TopicRegistry provides queue and subscription config for topics.
// Each topic can have a different queue backend.
type TopicRegistry struct {
	queues              map[Topic]queue.Queue
	subscriptionConfigs map[topicGroup]queue.SubscriptionConfig
}

// topicGroup identifies a topic and consumer group pair.
type topicGroup struct {
	topic         Topic
	consumerGroup string
}

// NewTopicRegistry creates a new TopicRegistry.
//   - topicConfigs: maps each topic to its queue backend
//   - subscriptionConfigs: subscription configurations for each topic+consumerGroup
func NewTopicRegistry(
	topicConfigs []TopicConfig,
	subscriptionConfigs []queue.SubscriptionConfig,
) TopicRegistry {
	queues := make(map[Topic]queue.Queue, len(topicConfigs))
	for _, tc := range topicConfigs {
		queues[tc.Topic] = tc.Queue
	}

	configs := make(map[topicGroup]queue.SubscriptionConfig, len(subscriptionConfigs))
	for _, cfg := range subscriptionConfigs {
		key := topicGroup{
			topic:         Topic(cfg.Topic),
			consumerGroup: cfg.ConsumerGroup,
		}
		configs[key] = cfg
	}

	return TopicRegistry{
		queues:              queues,
		subscriptionConfigs: configs,
	}
}

// Queue returns the queue backend for the given topic.
// Returns ok=false if no queue is registered for this topic.
func (r TopicRegistry) Queue(topic Topic) (queue.Queue, bool) {
	q, ok := r.queues[topic]
	return q, ok
}

// SubscriptionConfig returns the subscription configuration for the given topic and consumer group.
// Returns ok=false if no configuration is registered.
func (r TopicRegistry) SubscriptionConfig(topic Topic, consumerGroup string) (queue.SubscriptionConfig, bool) {
	cfg, ok := r.subscriptionConfigs[topicGroup{topic: topic, consumerGroup: consumerGroup}]
	return cfg, ok
}
