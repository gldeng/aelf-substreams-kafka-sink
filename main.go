package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"

	pbkafka "github.com/gldeng/substreams-sink-kafka-messages/pb/gldeng/substreams/sink/kafka/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

var expectedOutputModuleType = string(new(pbkafka.Publish).ProtoReflect().Descriptor().FullName())

var zlog, tracer = logging.RootLogger("project", "github.com/change_to_org/change_to_project")

func main() {
	logging.InstantiateLoggers()

	Run(
		"sinker",
		"Simple Go sinker sinking data to your terminal",

		Command(sinkRunE,
			"sink <endpoint> <manifest> [<output_module>]",
			"Run the sinker code",
			RangeArgs(2, 3),
			Flags(func(flags *pflag.FlagSet) {
				sink.AddFlagsToSet(flags)
			}),
		),

		OnCommandErrorLogAndExit(zlog),
	)
}

func sinkRunE(cmd *cobra.Command, args []string) error {
	endpoint := args[0]
	manifestPath := args[1]

	// Find the output module in the manifest sink.moduleName configuration. If you have no
	// such configuration, you can change the value below and set the module name explicitly.
	outputModuleName := sink.InferOutputModuleFromPackage
	if len(args) == 3 {
		outputModuleName = args[2]
	}

	sinker, err := sink.NewFromViper(
		cmd,
		// Should be the Protobuf full name of the map's module output, we use
		// `substreams-database-changes` imported type. Adjust to your needs.
		//
		// If your Protobuf is defined in your Substreams manifest, you can use `substream protogen`
		// while being in the same folder that contain `buf.gen.yaml` file in the example folder.
		expectedOutputModuleType,
		endpoint,
		manifestPath,
		outputModuleName,
		// This is the block range, in our case defined as Substreams module's start block and up forever
		":",
		zlog,
		tracer,
	)
	cli.NoError(err, "unable to create sinker: %s", err)

	sinker.OnTerminating(func(err error) {
		cli.NoError(err, "unexpected sinker error")

		zlog.Info("sink is terminating")
	})

	// You **must** save the cursor somewhere, saving it to memory while
	// make it last until the process is killed, in which on re-start, the
	// sinker will resume from start block again. You can simply read from
	// a file the string value of the cursor and use `sink.NewCursor(value)`
	// to load it.

	// Blocking call, will return on sinker termination
	sinker.Run(context.Background(), sink.NewBlankCursor(), sink.NewSinkerHandlers(handleBlockScopedData, handleBlockUndoSignal))
	return nil
}

func handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	_ = ctx

	payload := &pbkafka.Publish{}
	if err := data.Output.MapOutput.UnmarshalTo(payload); err != nil {
		return fmt.Errorf("unable to unmarshal database changes: %w", err)
	}

	fmt.Printf("Block #%d (%s) data received with %d changes\n", data.Clock.Number, data.Clock.Id, len(payload.TopicBundles))

	for _, bundle := range payload.TopicBundles {
		publish(bundle)
	}

	// Once you have processed the block, you **must** persist the cursor, persistence
	// can take differnet form. For example, you can save it to a file, or to a database.
	// You can simply use `os.WriteFile("cursor.txt", []byte(cursor.String()), 0644)` to
	// save it to a file.
	_ = cursor

	// The isLive boolean is set to true when the sinker is running in the live portion
	// of the chain. If there is **no** liveness checker configured on the sinker, the isLive
	// value is going to be nil.
	_ = isLive

	return nil
}

func handleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	_ = ctx

	// The chain forked for one or more blocks, you **must** rewind your changes back to the
	// last valid block, which is provided in the undo signal. You **must** also persist the
	// last valid cursor also provided in the undo signal. You can simply use
	// `os.WriteFile("cursor.txt", []byte(cursor.String()), 0644)` to
	// save it to a file.
	_ = cursor

	fmt.Printf("Rewinding changes back to block %s\n", undoSignal.LastValidBlock)
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

func publish(bundle *pbkafka.TopicBundle) {
	// Kafka producer configuration
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "PLAINTEXT://34.123.109.206:9092", // Replace with your Kafka broker(s)
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	for _, message := range bundle.Messages {

		valueBytes, err := getValueBytes(bundle.SchemaId, message)
		b64Key := base64.StdEncoding.EncodeToString(message.Key)
		b64Value := base64.StdEncoding.EncodeToString(valueBytes)
		println(b64Key + ":" + b64Value)
		if err != nil {
			fmt.Printf("Failed to serialize message: %s\n", err)
		} else {
			// Produce the message to Kafka
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &bundle.Topic, Partition: kafka.PartitionAny},
				Key:            message.Key,
				Value:          valueBytes,
			}, nil)

			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
			} else {
				fmt.Printf("Message produced successfully to topic %s\n", bundle.Topic)
			}
		}

	}

	// Wait for all messages in the producer queue to be delivered
	left := producer.Flush(10000) // Wait up to 10 seconds
	println("%d messages unpublished", left)
}
