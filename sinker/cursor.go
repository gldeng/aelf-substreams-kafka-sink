package sinker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	sink "github.com/streamingfast/substreams-sink"
)

var ErrCursorNotFound = errors.New("cursor not found")

var (
	_ CursorTracker = (*FileBasedCursorTracker)(nil)
)

type CursorTracker interface {
	GetCursor(ctx context.Context, outputModuleHash string) (*sink.Cursor, error)
	WriteCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error
}
type FileBasedCursorTracker struct {
	filePath string
}

// NewCursorTracker creates a new FileBasedCursorTracker with the specified file path.
func NewCursorTracker(filePath string) *FileBasedCursorTracker {
	return &FileBasedCursorTracker{filePath: filePath}
}

// GetCursor reads the cursor from the file.
func (ct *FileBasedCursorTracker) GetCursor(ctx context.Context, outputModuleHash string) (*sink.Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	data, err := os.ReadFile(ct.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCursorNotFound
		}
		return nil, fmt.Errorf("reading cursor from file: %w", err)
	}

	cursorStr := string(data)
	cursor, err := sink.NewCursor(cursorStr)
	if err != nil {
		return nil, fmt.Errorf("creating cursor from string: %w", err)
	}

	return cursor, nil
}

// WriteCursor writes the cursor to the file.
func (ct *FileBasedCursorTracker) WriteCursor(ctx context.Context, moduleHash string, c *sink.Cursor) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cursorStr := c.String()
	err := os.WriteFile(ct.filePath, []byte(cursorStr), 0644)
	if err != nil {
		return fmt.Errorf("writing cursor to file: %w", err)
	}

	return nil
}
