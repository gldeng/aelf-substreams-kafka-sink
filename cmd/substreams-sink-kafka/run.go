package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gldeng/aelf-substreams-kafka-sink/sinker"
	pbkafka "github.com/gldeng/substreams-sink-kafka-messages/pb/gldeng/substreams/sink/kafka/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"time"
)

var expectedOutputModuleType = string(new(pbkafka.Publish).ProtoReflect().Descriptor().FullName())

var sinkRunCmd = Command(sinkRunE,
	"run <bootstrap_servers> <cursor_tracker_file> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs MongoDB sink process",
	RangeArgs(5, 6),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	bootstrapServers := args[0]
	cursorTrackerFile := args[1]
	endpoint := args[2]
	manifestPath := args[3]
	outputModuleName := args[4]
	blockRange := ""
	if len(args) > 5 {
		blockRange = args[5]
	}

	// Kafka producer configuration
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return fmt.Errorf("creating kafka producer: %w", err)
	}
	defer producer.Close()

	sink, err := sink.NewFromViper(
		cmd,
		expectedOutputModuleType,
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)

	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	cursorTracker := sinker.NewCursorTracker(cursorTrackerFile)

	kafkaSinker, err := sinker.New(sink, producer, cursorTracker, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup kafka sinker: %w", err)
	}

	kafkaSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		kafkaSinker.Shutdown(err)
	})

	go func() {
		kafkaSinker.Run(ctx)
	}()

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
