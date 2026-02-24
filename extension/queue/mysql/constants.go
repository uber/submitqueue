package mysql

// Common constants for frequently repeated strings across stores

const (
	// Tag key (used in every Tagged() call)
	tagErrorType = "error_type"

	// Common log field names (used extensively across all stores)
	logTopic        = "topic"
	logPartitionKey = "partition_key"
	logMessageID    = "message_id"
	logError        = "error"

	// Error types used across multiple methods/stores
	errorBeginTx = "begin_transaction"
	errorCommit  = "commit"
)
