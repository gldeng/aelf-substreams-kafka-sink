package sinker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	pbkafka "github.com/gldeng/substreams-sink-kafka-messages/pb/gldeng/substreams/sink/kafka/v1"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type MongoSinker struct {
	*shutter.Shutter
	*sink.Sinker

	cursorTracker CursorTracker
	producer      *kafka.Producer
	logger        *zap.Logger
	tracer        logging.Tracer

	stats      *Stats
	lastCursor *sink.Cursor
}

func New(sink *sink.Sinker, producer *kafka.Producer, cursorTracker CursorTracker, logger *zap.Logger, tracer logging.Tracer) (*MongoSinker, error) {
	s := &MongoSinker{
		Shutter: shutter.New(),
		Sinker:  sink,

		cursorTracker: cursorTracker,
		producer:      producer,
		logger:        logger,
		tracer:        tracer,

		stats: NewStats(logger),
	}

	s.OnTerminating(func(err error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.writeLastCursor(ctx, err)
	})

	return s, nil
}

func (s *MongoSinker) writeLastCursor(ctx context.Context, err error) {
	if s.lastCursor == nil || err != nil {
		return
	}

	_ = s.cursorTracker.WriteCursor(ctx, s.OutputModuleHash(), s.lastCursor)
}

func (s *MongoSinker) Run(ctx context.Context) {
	cursor, err := s.cursorTracker.GetCursor(ctx, s.OutputModuleHash())
	if err != nil && !errors.Is(err, ErrCursorNotFound) {
		s.Shutdown(fmt.Errorf("unable to retrieve cursor: %w", err))
		return
	}

	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.stats.LogNow()
		s.logger.Info("mongodb sinker terminating", zap.Stringer("last_block_written", s.stats.lastBlock))
		s.Sinker.Shutdown(err)
	})

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })

	logEach := 15 * time.Second
	if s.logger.Core().Enabled(zap.DebugLevel) {
		logEach = 5 * time.Second
	}

	s.stats.Start(logEach, cursor)

	s.logger.Info("starting mongodb sink", zap.Duration("stats_refresh_each", logEach), zap.Stringer("restarting_at", cursor.Block()))
	s.Sinker.Run(ctx, cursor, s)
}

func (s *MongoSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	payload := &pbkafka.Publish{}
	err := proto.Unmarshal(output.GetMapOutput().GetValue(), payload)
	if err != nil {
		return fmt.Errorf("unmarshal publish payload: %w", err)
	}

	err = s.applyDatabaseChanges(ctx, dataAsBlockRef(data), payload)
	if err != nil {
		return fmt.Errorf("apply publish payload: %w", err)
	}

	s.lastCursor = cursor

	return nil
}

func (s *MongoSinker) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}

func (s *MongoSinker) applyDatabaseChanges(ctx context.Context, block bstream.BlockRef, payload *pbkafka.Publish) (err error) {
	startTime := time.Now()
	defer func() {
		FlushDuration.AddInt64(time.Since(startTime).Nanoseconds())
	}()

	count := 0
	for _, bundle := range payload.TopicBundles {
		for _, message := range bundle.Messages {
			valueBytes, err := getValueBytes(bundle.SchemaId, message)
			if err != nil {
				return fmt.Errorf("serializing message in topic %s with key %s: %w (Block %s)", bundle.Topic, message.Key, err, block)
			}
			// Produce the message to Kafka
			err = s.producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &bundle.Topic, Partition: kafka.PartitionAny},
				Key:            message.Key,
				Value:          valueBytes,
			}, nil)
			if err != nil {
				return fmt.Errorf("publishing message in topic %s with key %s: %w (Block %s)", bundle.Topic, message.Key, err, block)
			}
			count += 1
		}
	}
	s.producer.Flush(10000) // TODO: Can this fail
	FlushCount.Inc()
	FlushedEntriesCount.AddInt(count)
	s.stats.RecordBlock(block)

	return nil
}

func getValueBytes(schemaId int32, message *pbkafka.Message) ([]byte, error) {
	magicByte := 0
	// Build the complete message in the format: [MAGIC_BYTE][SCHEMA_ID][PROTOBUF_PAYLOAD]
	var buffer bytes.Buffer
	buffer.WriteByte(byte(magicByte))
	binary.Write(&buffer, binary.BigEndian, schemaId)
	buffer.WriteByte(byte(0)) // index array
	buffer.Write(message.Value)
	return buffer.Bytes(), nil
}

func dataAsBlockRef(blockData *pbsubstreamsrpc.BlockScopedData) bstream.BlockRef {
	return clockAsBlockRef(blockData.Clock)
}

func clockAsBlockRef(clock *pbsubstreams.Clock) bstream.BlockRef {
	return bstream.NewBlockRef(clock.Id, clock.Number)
}
