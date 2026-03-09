// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/uber/submitqueue/extension/queue/mysql/ctl/lib"
)

// confirmAction prompts the user for confirmation unless --no-interactive is set.
// Returns nil if confirmed, error if declined.
func confirmAction(noInteractive bool, message string) error {
	if noInteractive {
		return nil
	}
	fmt.Fprintf(os.Stderr, "%s [y/N]: ", message)
	reader := bufio.NewReader(os.Stdin)
	answer, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read confirmation: %w", err)
	}
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		return fmt.Errorf("operation cancelled")
	}
	return nil
}

// newRootCmd creates the root cobra command with all subcommands wired up.
// The returned command handles --dsn and --json persistent flags.
func newRootCmd() *cobra.Command {
	var (
		dsn           string
		jsonOut       bool
		noInteractive bool
		store         *lib.AdminStore
		db            *sql.DB
	)

	rootCmd := &cobra.Command{
		Use:   "queue-admin",
		Short: "Admin CLI for the MySQL queue extension",
		Long:  "Inspect, manage, and troubleshoot the MySQL-backed message queue.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if dsn == "" {
				dsn = os.Getenv("QUEUE_MYSQL_DSN")
			}
			if dsn == "" {
				return fmt.Errorf("--dsn flag or QUEUE_MYSQL_DSN env var is required")
			}
			var err error
			db, err = sql.Open("mysql", dsn)
			if err != nil {
				return fmt.Errorf("open database: %w", err)
			}
			if err := db.PingContext(cmd.Context()); err != nil {
				return fmt.Errorf("ping database: %w", err)
			}
			store = lib.NewAdminStore(db)
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if db != nil {
				return db.Close()
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&dsn, "dsn", "", "MySQL DSN (or set QUEUE_MYSQL_DSN)")
	rootCmd.PersistentFlags().BoolVar(&jsonOut, "json", false, "Output in JSON format")
	rootCmd.PersistentFlags().BoolVar(&noInteractive, "no-interactive", false, "Skip confirmation prompts (for scripting)")

	rootCmd.AddCommand(
		newListTopicsCmd(&store, &jsonOut),
		newTopicStatsCmd(&store, &jsonOut),
		newListMessagesCmd(&store, &jsonOut),
		newInspectMessageCmd(&store, &jsonOut),
		newDeleteMessageCmd(&store, &noInteractive),
		newPurgeTopicCmd(&store, &noInteractive),
		newListDLQCmd(&store, &jsonOut),
		newRequeueDLQCmd(&store),
		newPurgeDLQCmd(&store, &noInteractive),
		newListOffsetsCmd(&store, &jsonOut),
		newResetOffsetCmd(&store, &noInteractive),
		newListLeasesCmd(&store, &jsonOut),
		newConsumerLagCmd(&store, &jsonOut),
		newStaleLeasesCmd(&store, &jsonOut),
		newReleaseLeaseCmd(&store, &noInteractive),
	)

	return rootCmd
}

func newListTopicsCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	return &cobra.Command{
		Use:   "list-topics",
		Short: "List all topics with message counts",
		RunE: func(cmd *cobra.Command, args []string) error {
			topics, err := (*store).ListTopics(cmd.Context())
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, topics)
			}
			headers := []string{"TOPIC", "MESSAGES"}
			var rows [][]string
			for _, t := range topics {
				rows = append(rows, []string{t.Topic, strconv.FormatInt(t.MessageCount, 10)})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
}

func newTopicStatsCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var topic, dlqSuffix string
	cmd := &cobra.Command{
		Use:   "topic-stats",
		Short: "Show detailed statistics for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			stats, err := (*store).GetTopicStats(cmd.Context(), topic, dlqSuffix)
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, stats)
			}
			headers := []string{"FIELD", "VALUE"}
			rows := [][]string{
				{"Topic", stats.Topic},
				{"Total Messages", strconv.FormatInt(stats.TotalMessages, 10)},
				{"DLQ Count", strconv.FormatInt(stats.DLQCount, 10)},
				{"Partitions", strconv.FormatInt(stats.PartitionCount, 10)},
				{"Consumer Groups", strconv.FormatInt(stats.ConsumerGroupCount, 10)},
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&dlqSuffix, "dlq-suffix", "_dlq", "DLQ topic suffix")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newListMessagesCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var topic, partition string
	var limit int
	cmd := &cobra.Command{
		Use:   "list-messages",
		Short: "List messages for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			messages, err := (*store).ListMessages(cmd.Context(), topic, partition, limit)
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, messages)
			}
			headers := []string{"OFFSET", "ID", "PARTITION", "CREATED_AT", "PUBLISHED_AT"}
			var rows [][]string
			for _, m := range messages {
				rows = append(rows, []string{
					strconv.FormatInt(m.Offset, 10),
					m.ID,
					m.PartitionKey,
					lib.FormatMillis(m.CreatedAt),
					lib.FormatMillis(m.PublishedAt),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&partition, "partition", "", "Filter by partition key")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of messages to show")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newInspectMessageCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var topic, messageID string
	cmd := &cobra.Command{
		Use:   "inspect-message",
		Short: "Show full message details including payload and metadata",
		RunE: func(cmd *cobra.Command, args []string) error {
			detail, found, err := (*store).InspectMessage(cmd.Context(), topic, messageID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("message %q not found in topic %q", messageID, topic)
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, detail)
			}
			headers := []string{"FIELD", "VALUE"}
			rows := [][]string{
				{"Offset", strconv.FormatInt(detail.Offset, 10)},
				{"ID", detail.ID},
				{"Topic", detail.Topic},
				{"Partition", detail.PartitionKey},
				{"Created At", lib.FormatMillis(detail.CreatedAt)},
				{"Published At", lib.FormatMillis(detail.PublishedAt)},
				{"Payload", string(detail.Payload)},
			}
			if len(detail.Metadata) > 0 {
				for k, v := range detail.Metadata {
					rows = append(rows, []string{fmt.Sprintf("Metadata[%s]", k), v})
				}
			}
			if detail.FailedAt > 0 {
				rows = append(rows,
					[]string{"Failed At", lib.FormatMillis(detail.FailedAt)},
					[]string{"Failure Count", strconv.Itoa(detail.FailureCount)},
					[]string{"Last Error", detail.LastError},
					[]string{"Original Topic", detail.OriginalTopic},
				)
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&messageID, "message-id", "", "Message ID (required)")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("message-id")
	return cmd
}

func newDeleteMessageCmd(store **lib.AdminStore, noInteractive *bool) *cobra.Command {
	var topic, messageID string
	cmd := &cobra.Command{
		Use:   "delete-message",
		Short: "Delete a specific message",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmAction(*noInteractive, fmt.Sprintf("Delete message %q from topic %q?", messageID, topic)); err != nil {
				return err
			}
			affected, err := (*store).DeleteMessage(cmd.Context(), topic, messageID)
			if err != nil {
				return err
			}
			if affected == 0 {
				fmt.Fprintf(os.Stderr, "No message found with ID %q in topic %q\n", messageID, topic)
				return nil
			}
			fmt.Printf("Deleted message %q from topic %q\n", messageID, topic)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&messageID, "message-id", "", "Message ID (required)")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("message-id")
	return cmd
}

func newPurgeTopicCmd(store **lib.AdminStore, noInteractive *bool) *cobra.Command {
	var topic string
	cmd := &cobra.Command{
		Use:   "purge-topic",
		Short: "Delete all messages for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmAction(*noInteractive, fmt.Sprintf("Purge ALL messages from topic %q?", topic)); err != nil {
				return err
			}
			affected, err := (*store).PurgeTopic(cmd.Context(), topic)
			if err != nil {
				return err
			}
			fmt.Printf("Purged %d messages from topic %q\n", affected, topic)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newListDLQCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var topic, dlqSuffix string
	var limit int
	cmd := &cobra.Command{
		Use:   "list-dlq",
		Short: "List dead-letter queue messages for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqTopic := topic + dlqSuffix
			messages, err := (*store).ListMessages(cmd.Context(), dlqTopic, "", limit)
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, messages)
			}
			headers := []string{"OFFSET", "ID", "PARTITION", "CREATED_AT"}
			var rows [][]string
			for _, m := range messages {
				rows = append(rows, []string{
					strconv.FormatInt(m.Offset, 10),
					m.ID,
					m.PartitionKey,
					lib.FormatMillis(m.CreatedAt),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Original topic name (required)")
	cmd.Flags().StringVar(&dlqSuffix, "dlq-suffix", "_dlq", "DLQ topic suffix")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of messages to show")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newRequeueDLQCmd(store **lib.AdminStore) *cobra.Command {
	var topic, messageID, dlqSuffix string
	cmd := &cobra.Command{
		Use:   "requeue-dlq",
		Short: "Move a DLQ message back to its original topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := (*store).RequeueDLQ(cmd.Context(), topic, messageID, dlqSuffix); err != nil {
				return err
			}
			fmt.Printf("Requeued message %q from DLQ back to topic %q\n", messageID, topic)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Original topic name (required)")
	cmd.Flags().StringVar(&messageID, "message-id", "", "Message ID (required)")
	cmd.Flags().StringVar(&dlqSuffix, "dlq-suffix", "_dlq", "DLQ topic suffix")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("message-id")
	return cmd
}

func newPurgeDLQCmd(store **lib.AdminStore, noInteractive *bool) *cobra.Command {
	var topic, dlqSuffix string
	cmd := &cobra.Command{
		Use:   "purge-dlq",
		Short: "Delete all DLQ messages for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqTopic := topic + dlqSuffix
			if err := confirmAction(*noInteractive, fmt.Sprintf("Purge ALL messages from DLQ topic %q?", dlqTopic)); err != nil {
				return err
			}
			affected, err := (*store).PurgeTopic(cmd.Context(), dlqTopic)
			if err != nil {
				return err
			}
			fmt.Printf("Purged %d messages from DLQ topic %q\n", affected, dlqTopic)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Original topic name (required)")
	cmd.Flags().StringVar(&dlqSuffix, "dlq-suffix", "_dlq", "DLQ topic suffix")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newListOffsetsCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var consumerGroup string
	cmd := &cobra.Command{
		Use:   "list-offsets",
		Short: "Show consumer group offsets",
		RunE: func(cmd *cobra.Command, args []string) error {
			offsets, err := (*store).ListOffsets(cmd.Context(), consumerGroup)
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, offsets)
			}
			headers := []string{"CONSUMER_GROUP", "TOPIC", "PARTITION", "OFFSET_ACKED", "UPDATED_AT"}
			var rows [][]string
			for _, o := range offsets {
				rows = append(rows, []string{
					o.ConsumerGroup,
					o.Topic,
					o.PartitionKey,
					strconv.FormatInt(o.OffsetAcked, 10),
					lib.FormatMillis(o.UpdatedAt),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&consumerGroup, "consumer-group", "", "Filter by consumer group")
	return cmd
}

func newResetOffsetCmd(store **lib.AdminStore, noInteractive *bool) *cobra.Command {
	var consumerGroup, topic, partition string
	var offset int64
	cmd := &cobra.Command{
		Use:   "reset-offset",
		Short: "Reset consumer group offset for a partition",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmAction(*noInteractive, fmt.Sprintf("Reset offset to %d for consumer-group=%q topic=%q partition=%q?", offset, consumerGroup, topic, partition)); err != nil {
				return err
			}
			affected, err := (*store).ResetOffset(cmd.Context(), consumerGroup, topic, partition, offset)
			if err != nil {
				return err
			}
			if affected == 0 {
				fmt.Fprintf(os.Stderr, "No offset found for consumer-group=%q topic=%q partition=%q\n", consumerGroup, topic, partition)
				return nil
			}
			fmt.Printf("Reset offset to %d for consumer-group=%q topic=%q partition=%q\n", offset, consumerGroup, topic, partition)
			return nil
		},
	}
	cmd.Flags().StringVar(&consumerGroup, "consumer-group", "", "Consumer group name (required)")
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&partition, "partition", "", "Partition key (required)")
	cmd.Flags().Int64Var(&offset, "offset", 0, "New offset value (default 0)")
	cmd.MarkFlagRequired("consumer-group")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("partition")
	return cmd
}

func newListLeasesCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	return &cobra.Command{
		Use:   "list-leases",
		Short: "Show all active partition leases",
		RunE: func(cmd *cobra.Command, args []string) error {
			leases, err := (*store).ListLeases(cmd.Context())
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, leases)
			}
			headers := []string{"CONSUMER_GROUP", "TOPIC", "PARTITION", "LEASED_BY", "LEASED_AT", "RENEWED_AT"}
			var rows [][]string
			for _, l := range leases {
				rows = append(rows, []string{
					l.ConsumerGroup,
					l.Topic,
					l.PartitionKey,
					l.LeasedBy,
					lib.FormatMillis(l.LeasedAt),
					lib.FormatMillis(l.LeaseRenewedAt),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
}

func newConsumerLagCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var topic string
	cmd := &cobra.Command{
		Use:   "consumer-lag",
		Short: "Show per-partition consumer lag for a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			lags, err := (*store).ConsumerLag(cmd.Context(), topic)
			if err != nil {
				return err
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, lags)
			}
			headers := []string{"CONSUMER_GROUP", "TOPIC", "PARTITION", "ACKED", "LATEST", "LAG"}
			var rows [][]string
			for _, l := range lags {
				rows = append(rows, []string{
					l.ConsumerGroup,
					l.Topic,
					l.PartitionKey,
					strconv.FormatInt(l.AckedOffset, 10),
					strconv.FormatInt(l.LatestOffset, 10),
					strconv.FormatInt(l.Lag, 10),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func newStaleLeasesCmd(store **lib.AdminStore, jsonOut *bool) *cobra.Command {
	var thresholdMs int64
	cmd := &cobra.Command{
		Use:   "stale-leases",
		Short: "Show partition leases not renewed within a threshold",
		RunE: func(cmd *cobra.Command, args []string) error {
			leases, err := (*store).StaleLeases(cmd.Context(), thresholdMs)
			if err != nil {
				return err
			}
			if len(leases) == 0 {
				fmt.Println("No stale leases found.")
				return nil
			}
			if *jsonOut {
				return lib.FormatJSON(os.Stdout, leases)
			}
			headers := []string{"CONSUMER_GROUP", "TOPIC", "PARTITION", "LEASED_BY", "LEASED_AT", "RENEWED_AT"}
			var rows [][]string
			for _, l := range leases {
				rows = append(rows, []string{
					l.ConsumerGroup,
					l.Topic,
					l.PartitionKey,
					l.LeasedBy,
					lib.FormatMillis(l.LeasedAt),
					lib.FormatMillis(l.LeaseRenewedAt),
				})
			}
			lib.FormatTable(os.Stdout, headers, rows)
			return nil
		},
	}
	cmd.Flags().Int64Var(&thresholdMs, "threshold", 60000, "Staleness threshold in milliseconds (default 60s)")
	return cmd
}

func newReleaseLeaseCmd(store **lib.AdminStore, noInteractive *bool) *cobra.Command {
	var consumerGroup, topic, partition string
	cmd := &cobra.Command{
		Use:   "release-lease",
		Short: "Force-release a partition lease",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := confirmAction(*noInteractive, fmt.Sprintf("Release lease for consumer-group=%q topic=%q partition=%q?", consumerGroup, topic, partition)); err != nil {
				return err
			}
			affected, err := (*store).ReleaseLease(cmd.Context(), consumerGroup, topic, partition)
			if err != nil {
				return err
			}
			if affected == 0 {
				fmt.Fprintf(os.Stderr, "No lease found for consumer-group=%q topic=%q partition=%q\n", consumerGroup, topic, partition)
				return nil
			}
			fmt.Printf("Released lease for consumer-group=%q topic=%q partition=%q\n", consumerGroup, topic, partition)
			return nil
		},
	}
	cmd.Flags().StringVar(&consumerGroup, "consumer-group", "", "Consumer group name (required)")
	cmd.Flags().StringVar(&topic, "topic", "", "Topic name (required)")
	cmd.Flags().StringVar(&partition, "partition", "", "Partition key (required)")
	cmd.MarkFlagRequired("consumer-group")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("partition")
	return cmd
}
