package mysql

import "fmt"

// validateTopicName ensures topic name is safe for use as a SQL column value
func validateTopicName(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if len(topic) > 255 {
		return fmt.Errorf("topic name too long (max 255 characters)")
	}
	// Only allow lowercase letters, numbers, underscores, and hyphens
	for _, c := range topic {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return fmt.Errorf("topic name must contain only lowercase letters, numbers, underscores, and hyphens")
		}
	}
	return nil
}
