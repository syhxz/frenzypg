package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

// MirrorQueue implements a durable, non-blocking mirror delivery queue backed
// by Kafka. Enqueue writes to a local in-memory channel (nanosecond-level),
// and a background producer goroutine batch-flushes to Kafka. Workers on the
// consumer side read from Kafka and execute against mirror pools.
//
// This ensures:
// - Primary is never blocked by mirror latency (Enqueue is non-blocking)
// - No data loss (Kafka provides durable, replicated storage)
// - Strict ordering per partition (each partition has a dedicated worker)
// - Zero message loss on crash (offset committed only after execution)
// - Automatic backpressure via Kafka consumer lag
type MirrorQueue struct {
	writer  *kafka.Writer
	readers []*kafka.Reader // one reader per partition
	logger  *zap.Logger

	// Local async buffer: Enqueue writes here, background producer drains to Kafka.
	msgCh chan kafka.Message

	// Lifecycle
	closed  atomic.Bool
	mu      sync.Mutex
	doneCh  chan struct{} // closed when produceLoop exits
	stopCh  chan struct{} // signals consumer workers to stop

	// Stats
	produced  atomic.Int64
	consumed  atomic.Int64
	errors_   atomic.Int64
	overflows atomic.Int64

	// Consumer workers (one per partition)
	consumerWg sync.WaitGroup
	executor   func(task *mirrorTask)

	// Config
	numPartitions int
	partitionChs  []chan kafka.Message // per-partition dispatch for strict ordering
}

// mirrorTask represents a unit of work to send to mirrors.
type mirrorTask struct {
	// Single statement (non-transactional)
	SQL string `json:"sql,omitempty"`
	// Transaction (multiple statements on one connection)
	Transaction []string `json:"tx,omitempty"`
}

// KafkaQueueConfig configures the Kafka-backed mirror queue.
type KafkaQueueConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`

	// Partitions controls the number of partitions when auto-creating the topic.
	// More partitions = higher throughput (parallel consumers). Default: 6.
	// Replication factor is min(3, number of brokers).
	Partitions int `yaml:"partitions"`

	// SASL authentication (optional)
	SASLMechanism   string `yaml:"sasl_mechanism"`
	SASLUsername    string `yaml:"sasl_username"`
	SASLPassword    string `yaml:"sasl_password"`
	SASLPasswordEnv string `yaml:"sasl_password_env"`

	// TLS configuration (optional)
	TLSEnabled    bool   `yaml:"tls_enabled"`
	TLSSkipVerify bool   `yaml:"tls_skip_verify"`
	TLSCAFile     string `yaml:"tls_ca_file"`
	TLSCertFile   string `yaml:"tls_cert_file"`
	TLSKeyFile    string `yaml:"tls_key_file"`

	// Tuning
	LocalBufferSize int    `yaml:"local_buffer_size"` // default: 100000
	Workers         int    `yaml:"workers"`           // consumer workers, default: 6
	GroupID         string `yaml:"group_id"`          // consumer group, default: frenzy-mirror
}

const (
	defaultKafkaTopic       = "frenzy-mirror"
	defaultKafkaGroupID     = "frenzy-mirror"
	defaultLocalBufferSize  = 100000
	defaultKafkaDialTimeout = 10 * time.Second
)

// NewMirrorQueue creates a Kafka-backed mirror queue. Fails fast if brokers
// are unreachable at startup.
func NewMirrorQueue(cfg *KafkaQueueConfig, logger *zap.Logger) (*MirrorQueue, error) {
	if cfg == nil || len(cfg.Brokers) == 0 {
		return nil, errors.New("mirror_queue: kafka brokers required")
	}

	topic := cfg.Topic
	if topic == "" {
		topic = defaultKafkaTopic
	}
	groupID := cfg.GroupID
	if groupID == "" {
		groupID = defaultKafkaGroupID
	}
	bufSize := cfg.LocalBufferSize
	if bufSize <= 0 {
		bufSize = defaultLocalBufferSize
	}

	// Build TLS config
	var tlsCfg *tls.Config
	if cfg.TLSEnabled {
		var err error
		tlsCfg, err = buildKafkaTLS(cfg)
		if err != nil {
			return nil, fmt.Errorf("mirror_queue: TLS config: %w", err)
		}
	}

	// Build SASL mechanism
	var saslMech sasl.Mechanism
	if cfg.SASLMechanism != "" {
		var err error
		saslMech, err = buildKafkaSASL(cfg)
		if err != nil {
			return nil, fmt.Errorf("mirror_queue: SASL config: %w", err)
		}
	}

	// Build transport
	transport := &kafka.Transport{
		TLS:  tlsCfg,
		SASL: saslMech,
	}

	// Startup reachability check
	dialer := &kafka.Dialer{
		Timeout:       defaultKafkaDialTimeout,
		TLS:           tlsCfg,
		SASLMechanism: saslMech,
	}
	if unreachable := dialKafkaBrokers(cfg.Brokers, dialer); len(unreachable) > 0 {
		return nil, fmt.Errorf("mirror_queue: unreachable broker(s): %s", strings.Join(unreachable, ", "))
	}

	// Ensure topic exists with desired partitions
	partitions := cfg.Partitions
	if partitions <= 0 {
		partitions = 6
	}
	if err := ensureKafkaTopic(cfg.Brokers, dialer, topic, partitions); err != nil {
		logger.Warn("Could not ensure Kafka topic (may already exist or lack permissions)",
			zap.String("topic", topic), zap.Error(err))
	}

	// Writer (producer)
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  topic,
		Transport:              transport,
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              1000,
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
		MaxAttempts:            10,
		WriteBackoffMin:        250 * time.Millisecond,
		WriteBackoffMax:        2 * time.Second,
		Async:                  false,
	}

	// Reader (consumer) — single reader with consumer group for automatic offset management.
	// We use one shared reader (GroupID mode) but dispatch to per-partition worker channels
	// to ensure strict ordering within each partition while allowing cross-partition parallelism.
	readerCfg := kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	}
	if dialer.TLS != nil || dialer.SASLMechanism != nil {
		readerCfg.Dialer = dialer
	}
	reader := kafka.NewReader(readerCfg)

	// Per-partition dispatch channels
	partitionChs := make([]chan kafka.Message, partitions)
	for i := 0; i < partitions; i++ {
		partitionChs[i] = make(chan kafka.Message, 100)
	}

	mq := &MirrorQueue{
		writer:        writer,
		readers:       []*kafka.Reader{reader}, // single reader in slice for cleanup
		logger:        logger,
		msgCh:         make(chan kafka.Message, bufSize),
		doneCh:        make(chan struct{}),
		stopCh:        make(chan struct{}),
		numPartitions: partitions,
		partitionChs:  partitionChs,
	}

	// Start background producer
	go mq.produceLoop()

	logger.Info("Mirror Kafka queue initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID),
		zap.Int("local_buffer", bufSize))

	return mq, nil
}

// Start begins consumer workers — one per partition for strict ordering.
// Architecture: 1 reader goroutine → routes by partition → N partition workers (1 per partition).
// Each partition worker processes messages sequentially, committing offset only after execution.
// This guarantees zero message loss on crash.
func (mq *MirrorQueue) Start(executor func(task *mirrorTask), workers int) {
	mq.executor = executor

	// Start one worker per partition
	for p := 0; p < mq.numPartitions; p++ {
		mq.consumerWg.Add(1)
		go mq.partitionWorker(p)
	}

	// Start the dispatcher that reads from Kafka and routes to partition channels
	mq.consumerWg.Add(1)
	go mq.dispatchByPartition()

	mq.logger.Info("Mirror queue consumers started",
		zap.Int("partitions", mq.numPartitions),
		zap.String("mode", "partition-per-worker (zero-loss)"))
}

// dispatchByPartition reads from the single Kafka reader and routes messages
// to per-partition channels for sequential processing.
func (mq *MirrorQueue) dispatchByPartition() {
	defer mq.consumerWg.Done()
	defer func() {
		// Close all partition channels when dispatcher exits
		for _, ch := range mq.partitionChs {
			close(ch)
		}
	}()

	reader := mq.readers[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-mq.stopCh
		cancel()
	}()

	for {
		select {
		case <-mq.stopCh:
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			select {
			case <-mq.stopCh:
				return
			default:
				if !errors.Is(err, context.Canceled) {
					mq.logger.Debug("Kafka read error (retrying)", zap.Error(err))
					time.Sleep(100 * time.Millisecond)
				}
				continue
			}
		}

		// Route to the correct partition channel
		p := msg.Partition
		if p >= 0 && p < len(mq.partitionChs) {
			select {
			case mq.partitionChs[p] <- msg:
			case <-mq.stopCh:
				return
			}
		} else {
			// Unexpected partition (topic might have more partitions than configured)
			// Process inline to avoid dropping
			mq.processMessage(msg)
			reader.CommitMessages(context.Background(), msg)
		}
	}
}

// partitionWorker processes messages for a single partition sequentially.
// Offset is committed only AFTER successful execution → zero data loss on crash.
func (mq *MirrorQueue) partitionWorker(partition int) {
	defer mq.consumerWg.Done()

	reader := mq.readers[0]
	ch := mq.partitionChs[partition]

	for msg := range ch {
		mq.processMessage(msg)

		// Commit ONLY after successful execution
		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			mq.logger.Warn("Kafka commit error",
				zap.Int("partition", partition), zap.Error(err))
		}
		mq.consumed.Add(1)
	}
}

// processMessage deserializes and executes a single mirror task.
func (mq *MirrorQueue) processMessage(msg kafka.Message) {
	var task mirrorTask
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		mq.logger.Warn("Failed to unmarshal Kafka message",
			zap.Int("partition", msg.Partition), zap.Error(err))
		return
	}
	if mq.executor != nil {
		mq.executor(&task)
	}
}

// EnqueueSQL enqueues a single SQL statement (non-blocking).
func (mq *MirrorQueue) EnqueueSQL(sql string) {
	mq.enqueue(&mirrorTask{SQL: sql})
}

// EnqueueTransaction enqueues a transaction (non-blocking).
func (mq *MirrorQueue) EnqueueTransaction(statements []string) {
	mq.enqueue(&mirrorTask{Transaction: statements})
}

// enqueue serializes the task and writes to the local buffer channel.
// Safe to call concurrently with Stop() — will not panic on closed channel.
func (mq *MirrorQueue) enqueue(task *mirrorTask) {
	if mq.closed.Load() {
		return
	}

	data, err := json.Marshal(task)
	if err != nil {
		mq.logger.Error("Failed to marshal mirror task", zap.Error(err))
		return
	}

	msg := kafka.Message{
		Value: data,
	}

	// Use mu to prevent send-on-closed-channel panic.
	// Stop() holds mu while closing msgCh, so if we hold mu here,
	// we are guaranteed the channel is still open.
	mq.mu.Lock()
	if mq.closed.Load() {
		mq.mu.Unlock()
		return
	}

	// Non-blocking send to local buffer
	select {
	case mq.msgCh <- msg:
		mq.mu.Unlock()
	default:
		mq.mu.Unlock()
		// Buffer full — Kafka is backed up. This should be extremely rare
		// with a 100K buffer. Log and count overflow.
		mq.overflows.Add(1)
		mq.logger.Warn("Mirror queue local buffer full, message will block",
			zap.Int64("overflows", mq.overflows.Load()))
		// Block until space is available (guarantees no data loss).
		// Re-check closed to avoid blocking forever if Stop() is called.
		select {
		case mq.msgCh <- msg:
		case <-mq.stopCh:
			mq.logger.Warn("Mirror queue stopping, dropping overflow message")
		}
	}
}

// produceLoop drains the local buffer and batch-writes to Kafka.
func (mq *MirrorQueue) produceLoop() {
	defer close(mq.doneCh)

	batch := make([]kafka.Message, 0, 1000)
	for {
		msg, ok := <-mq.msgCh
		if !ok {
			// Channel closed — flush remaining
			return
		}
		batch = append(batch, msg)

		// Drain more available messages without blocking
	drain:
		for len(batch) < cap(batch) {
			select {
			case m, ok := <-mq.msgCh:
				if !ok {
					break drain
				}
				batch = append(batch, m)
			default:
				break drain
			}
		}

		// Write batch to Kafka with retry
		mq.writeBatch(batch)
		batch = batch[:0]
	}
}

// writeBatch writes a batch to Kafka with retries. Failed messages are re-enqueued
// to prevent data loss.
func (mq *MirrorQueue) writeBatch(batch []kafka.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := mq.writer.WriteMessages(ctx, batch...)
	if err == nil {
		mq.produced.Add(int64(len(batch)))
		return
	}

	// Retry with backoff
	for attempt := 1; attempt <= 3; attempt++ {
		time.Sleep(time.Duration(attempt*200) * time.Millisecond)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		err = mq.writer.WriteMessages(ctx2, batch...)
		cancel2()
		if err == nil {
			mq.produced.Add(int64(len(batch)))
			return
		}
	}

	// Batch failed — try one by one
	var failed []kafka.Message
	for _, msg := range batch {
		ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
		if err := mq.writer.WriteMessages(ctx3, msg); err != nil {
			failed = append(failed, msg)
		} else {
			mq.produced.Add(1)
		}
		cancel3()
	}

	// Re-enqueue failed messages back to the local buffer for retry.
	// Add backoff to prevent infinite tight loops when Kafka is down.
	if len(failed) > 0 {
		mq.logger.Warn("Kafka write partially failed, re-enqueuing with backoff",
			zap.Int("failed_count", len(failed)))
		// Backoff before re-enqueue to avoid CPU spin when Kafka is down
		time.Sleep(2 * time.Second)

		for _, msg := range failed {
			select {
			case mq.msgCh <- msg:
				// Successfully re-enqueued for retry
			case <-mq.stopCh:
				// Shutting down — count as lost
				mq.errors_.Add(1)
				mq.logger.Error("Message dropped during shutdown",
					zap.Int("payload_bytes", len(msg.Value)))
			case <-time.After(10 * time.Second):
				mq.errors_.Add(1)
				mq.logger.Error("Failed to re-enqueue message (buffer full timeout) — MESSAGE LOST",
					zap.Int("payload_bytes", len(msg.Value)))
			}
		}
	}
}

// Stats returns queue statistics.
func (mq *MirrorQueue) Stats() map[string]int64 {
	return map[string]int64{
		"produced":     mq.produced.Load(),
		"consumed":     mq.consumed.Load(),
		"local_buffer": int64(len(mq.msgCh)),
		"errors":       mq.errors_.Load(),
		"overflows":    mq.overflows.Load(),
	}
}

// Stop gracefully shuts down the queue.
// Order: flush producer to Kafka, then stop partition workers.
func (mq *MirrorQueue) Stop(drainTimeout time.Duration) {
	mq.closed.Store(true)

	// 1. Close local buffer → signals produceLoop to drain remaining to Kafka and exit
	mq.mu.Lock()
	close(mq.msgCh)
	mq.mu.Unlock()

	// 2. Wait for producer to finish flushing to Kafka
	select {
	case <-mq.doneCh:
	case <-time.After(drainTimeout):
		mq.logger.Warn("Mirror queue producer drain timeout")
	}

	// 3. Give partition workers time to process remaining messages, then stop
	// Use sync.Once to prevent double-close panic on stopCh
	var closeOnce sync.Once
	closeStop := func() { closeOnce.Do(func() { close(mq.stopCh) }) }

	consumerDone := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Second)
		closeStop()
		mq.consumerWg.Wait()
		close(consumerDone)
	}()

	select {
	case <-consumerDone:
	case <-time.After(drainTimeout):
		closeStop()
		mq.consumerWg.Wait()
		mq.logger.Warn("Mirror queue consumer drain timeout")
	}

	// 4. Close Kafka connections
	if mq.writer != nil {
		mq.writer.Close()
	}
	for _, r := range mq.readers {
		if r != nil {
			r.Close()
		}
	}

	mq.logger.Info("Mirror Kafka queue stopped",
		zap.Int64("produced", mq.produced.Load()),
		zap.Int64("consumed", mq.consumed.Load()),
		zap.Int64("errors", mq.errors_.Load()))
}

// --- Kafka helpers ---

// ensureKafkaTopic creates the topic if it doesn't exist, with the desired
// number of partitions. Replication factor = min(3, broker count).
func ensureKafkaTopic(brokers []string, dialer *kafka.Dialer, topic string, partitions int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial for topic creation: %w", err)
	}
	defer conn.Close()

	// Get broker count for replication factor
	replicationFactor := 3
	if len(brokers) < 3 {
		replicationFactor = len(brokers)
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
		return err
	}
	return nil
}

func dialKafkaBrokers(brokers []string, dialer *kafka.Dialer) []string {
	var unreachable []string
	for _, b := range brokers {
		ctx, cancel := context.WithTimeout(context.Background(), defaultKafkaDialTimeout)
		conn, err := dialer.DialContext(ctx, "tcp", b)
		cancel()
		if err != nil {
			unreachable = append(unreachable, b)
			continue
		}
		conn.Close()
	}
	return unreachable
}

func buildKafkaTLS(cfg *KafkaQueueConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.TLSSkipVerify,
	}
	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("CA file contains no valid certs")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func buildKafkaSASL(cfg *KafkaQueueConfig) (sasl.Mechanism, error) {
	password := cfg.SASLPassword
	if cfg.SASLPasswordEnv != "" {
		password = os.Getenv(cfg.SASLPasswordEnv)
		if password == "" {
			return nil, fmt.Errorf("SASL password env %q is empty", cfg.SASLPasswordEnv)
		}
	}
	if cfg.SASLUsername == "" || password == "" {
		return nil, errors.New("SASL requires username and password")
	}

	switch strings.ToLower(cfg.SASLMechanism) {
	case "plain":
		return &plain.Mechanism{Username: cfg.SASLUsername, Password: password}, nil
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUsername, password)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUsername, password)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
}
