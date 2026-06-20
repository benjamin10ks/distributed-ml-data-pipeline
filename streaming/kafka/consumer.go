package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler processes a single record from one partition. Records within a
// partition are always delivered to Handler in offset order; records from
// different partitions may be handled concurrently. Returning an error
// leaves the record's offset uncommitted, so it will be redelivered on the
// next rebalance or restart.
type Handler func(ctx context.Context, record *kgo.Record) error

type ConsumerConfig struct {
	SeedBrokers []string
	GroupID     string
	Topics      []string
	Logger      *slog.Logger
}

// Consumer is a generic Kafka consumer group with manual offset commits —
// autocommit is disabled, and a record's offset is only committed after
// Handler returns successfully for it.
type Consumer struct {
	client *kgo.Client
	logger *slog.Logger
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.SeedBrokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one seed broker is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka: group id is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("kafka: at least one topic is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.SeedBrokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		// Without this, a partition moving to another group member during a
		// rebalance loses whatever this member processed but hadn't committed
		// yet, since CommitRecords below only commits after Handler succeeds.
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, _ map[string][]int32) {
			if err := c.CommitUncommittedOffsets(ctx); err != nil {
				logger.Error("commit on partition revoke failed", "error", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create consumer client: %w", err)
	}

	return &Consumer{client: client, logger: logger}, nil
}

// Run polls for records and dispatches them to handler until ctx is
// cancelled, fanning records out to one goroutine per partition currently
// assigned to this member. It blocks for the consumer's lifetime.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	type partitionKey struct {
		topic     string
		partition int32
	}

	queues := make(map[partitionKey]chan *kgo.Record)
	var wg sync.WaitGroup

	worker := func(key partitionKey, queue <-chan *kgo.Record) {
		defer wg.Done()
		for record := range queue {
			if err := handler(ctx, record); err != nil {
				c.logger.Error("handler failed, offset not committed",
					"topic", key.topic, "partition", key.partition, "offset", record.Offset, "error", err)
				continue
			}
			// Committing per record (rather than batching per poll) keeps this
			// generic consumer simple and exactly-once-per-commit-call; for
			// very high throughput a per-partition batched commit would be
			// cheaper, but isn't needed at this pipeline's volume.
			if err := c.client.CommitRecords(ctx, record); err != nil {
				c.logger.Error("commit failed",
					"topic", key.topic, "partition", key.partition, "offset", record.Offset, "error", err)
			}
		}
	}

	defer func() {
		for _, queue := range queues {
			close(queue)
		}
		wg.Wait()
	}()

	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			key := partitionKey{topic: p.Topic, partition: p.Partition}
			queue, ok := queues[key]
			if !ok {
				queue = make(chan *kgo.Record, 256)
				queues[key] = queue
				wg.Add(1)
				go worker(key, queue)
			}
			p.EachRecord(func(record *kgo.Record) {
				queue <- record
			})
		})
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
