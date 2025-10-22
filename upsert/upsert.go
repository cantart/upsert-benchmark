package upsert

import "context"

type Upserter interface {
	Upsert(ctx context.Context, table string, columns []string, rows [][]any, uniqueKeys []string) error
}
