package upsert

import (
	"context"
	"database/sql"
	"errors"
)

type BatchedHashIndexedUpserter struct {
	db        *sql.DB
	batchSize int
}

func NewBatchedHashIndexedUpserter(db *sql.DB) Upserter {
	return &BatchedHashIndexedUpserter{db: db, batchSize: 500}
}

// WithBatchSize returns a shallow copy with an overridden batch size for testing and tuning.
func (b *BatchedHashIndexedUpserter) WithBatchSize(size int) Upserter {
	clone := *b
	clone.batchSize = size
	return &clone
}

func (b *BatchedHashIndexedUpserter) Upsert(ctx context.Context, table string, columns []string, rows [][]any, uniqueKeys []string) error {
	if len(columns) == 0 {
		return errors.New("at least one column is required")
	}
	if len(uniqueKeys) == 0 {
		return errors.New("at least one unique key is required")
	}
	if len(rows) == 0 {
		return nil
	}
	if b.batchSize <= 0 {
		return errors.New("batch size must be positive")
	}

	mut := NewHashIndexedUpserter(b.db)
	for start := 0; start < len(rows); start += b.batchSize {
		end := min(start+b.batchSize, len(rows))
		if err := mut.Upsert(ctx, table, columns, rows[start:end], uniqueKeys); err != nil {
			return err
		}
	}
	return nil
}
