//go:build integration

package upsert

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

const defaultIntegrationDSN = "postgres://postgres:postgres@localhost:5432/upsertbenchmark?sslmode=disable"

func BenchmarkRealUpserters(b *testing.B) {
	dsn := os.Getenv("UPSERT_BENCHMARK_DSN")
	if dsn == "" {
		dsn = defaultIntegrationDSN
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		b.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		b.Skipf("skipping integration benchmarks: %v", err)
	}

	const tableName = "bench_real_users"
	tableIdent, err := quoteIdentifier(tableName)
	if err != nil {
		b.Fatalf("quoteIdentifier: %v", err)
	}

	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id BIGINT PRIMARY KEY, name TEXT NOT NULL)", tableIdent)
	if _, err := db.ExecContext(ctx, createStmt); err != nil {
		b.Fatalf("create table: %v", err)
	}

	rowCounts := []int{128, 1024, 4096}
	columns := []string{"id", "name"}
	uniqueKeys := []string{"id"}

	for _, count := range rowCounts {
		rows := generateIntegrationRows(count)
		half := len(rows) / 2

		b.Run(fmt.Sprintf("rows=%d/Naive", count), func(b *testing.B) {
			runIntegrationBenchmark(b, db, tableName, tableIdent, columns, uniqueKeys, rows, half, NewNaiveUpserter(db))
		})

		b.Run(fmt.Sprintf("rows=%d/HashIndexed", count), func(b *testing.B) {
			runIntegrationBenchmark(b, db, tableName, tableIdent, columns, uniqueKeys, rows, half, NewHashIndexedUpserter(db))
		})

		b.Run(fmt.Sprintf("rows=%d/BatchedHashIndexed", count), func(b *testing.B) {
			base := NewBatchedHashIndexedUpserter(db).(*BatchedHashIndexedUpserter)
			upserter := base.WithBatchSize(512)
			runIntegrationBenchmark(b, db, tableName, tableIdent, columns, uniqueKeys, rows, half, upserter)
		})
	}
}

func runIntegrationBenchmark(b *testing.B, db *sql.DB, tableName, tableIdent string, columns, uniqueKeys []string, rows [][]any, seedCount int, upserter Upserter) {
	ctx := context.Background()
	truncateStmt := fmt.Sprintf("TRUNCATE %s", tableIdent)
	seedStmt := fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", tableIdent)

	b.Helper()
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if _, err := db.ExecContext(ctx, truncateStmt); err != nil {
			b.Fatalf("truncate %s: %v", tableName, err)
		}
		for j := 0; j < seedCount; j++ {
			if _, err := db.ExecContext(ctx, seedStmt, rows[j][0], rows[j][1]); err != nil {
				b.Fatalf("seed row: %v", err)
			}
		}
		b.StartTimer()
		if err := upserter.Upsert(ctx, tableName, columns, rows, uniqueKeys); err != nil {
			b.Fatalf("Upsert: %v", err)
		}
	}
	b.StopTimer()

	if _, err := db.ExecContext(ctx, truncateStmt); err != nil {
		b.Fatalf("cleanup truncate %s: %v", tableName, err)
	}
}

func generateIntegrationRows(count int) [][]any {
	rows := make([][]any, count)
	for i := 0; i < count; i++ {
		rows[i] = []any{int64(i + 1), fmt.Sprintf("name-%d", i+1)}
	}
	return rows
}
