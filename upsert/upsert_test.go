package upsert

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func BenchmarkUpserters(b *testing.B) {
	rowCounts := []int{1, 32, 128}
	for _, count := range rowCounts {
		rows := generateBenchmarkRows(count)
		name := fmt.Sprintf("rows=%d", count)
		b.Run(name, func(b *testing.B) {
			b.Run("Naive", func(b *testing.B) {
				benchmarkNaiveUpserter(b, rows)
			})
			b.Run("HashIndexed", func(b *testing.B) {
				benchmarkHashIndexedUpserter(b, rows)
			})
			b.Run("BatchedHashIndexed", func(b *testing.B) {
				benchmarkBatchedHashIndexedUpserter(b, rows, 128)
			})
		})
	}
}

func benchmarkNaiveUpserter(b *testing.B, rows [][]any) {
	b.Helper()
	b.ReportAllocs()

	columns := []string{"id", "name"}
	uniqueKeys := []string{"id"}
	ctx := context.Background()

	for b.Loop() {
		b.StopTimer()
		db, mock, err := sqlmock.New()
		if err != nil {
			b.Fatalf("sqlmock.New: %v", err)
		}
		upserter := NewNaiveUpserter(db)

		mock.ExpectBegin()
		for _, row := range rows {
			mock.ExpectQuery("SELECT 1 FROM .*").
				WithArgs(row[0]).
				WillReturnError(sql.ErrNoRows)
			mock.ExpectExec("INSERT INTO .*").
				WithArgs(driverArgs(row)...).
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		mock.ExpectCommit()
		mock.ExpectClose()

		b.StartTimer()
		if err := upserter.Upsert(ctx, "users", columns, rows, uniqueKeys); err != nil {
			b.Fatalf("Upsert: %v", err)
		}
		b.StopTimer()

		if err := db.Close(); err != nil {
			b.Fatalf("db.Close: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			b.Fatalf("unmet expectations: %v", err)
		}
		b.StartTimer()
	}
}

func benchmarkHashIndexedUpserter(b *testing.B, rows [][]any) {
	b.Helper()
	b.ReportAllocs()

	columns := []string{"id", "name"}
	uniqueKeys := []string{"id"}
	ctx := context.Background()

	args := flattenDriverValues(rows)

	for b.Loop() {
		b.StopTimer()
		db, mock, err := sqlmock.New()
		if err != nil {
			b.Fatalf("sqlmock.New: %v", err)
		}
		upserter := NewHashIndexedUpserter(db)

		mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("INSERT INTO .*").
			WithArgs(args...).
			WillReturnResult(sqlmock.NewResult(0, int64(len(rows))))
		mock.ExpectClose()

		b.StartTimer()
		if err := upserter.Upsert(ctx, "users", columns, rows, uniqueKeys); err != nil {
			b.Fatalf("Upsert: %v", err)
		}
		b.StopTimer()

		if err := db.Close(); err != nil {
			b.Fatalf("db.Close: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			b.Fatalf("unmet expectations: %v", err)
		}
		b.StartTimer()
	}
}

func benchmarkBatchedHashIndexedUpserter(b *testing.B, rows [][]any, batchSize int) {
	b.Helper()
	b.ReportAllocs()

	columns := []string{"id", "name"}
	uniqueKeys := []string{"id"}
	ctx := context.Background()

	for b.Loop() {
		b.StopTimer()
		db, mock, err := sqlmock.New()
		if err != nil {
			b.Fatalf("sqlmock.New: %v", err)
		}
		base := NewBatchedHashIndexedUpserter(db).(*BatchedHashIndexedUpserter)
		upserter := base.WithBatchSize(batchSize)

		for start := 0; start < len(rows); start += batchSize {
			end := min(start+batchSize, len(rows))
			chunk := rows[start:end]
			mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("INSERT INTO .*").
				WithArgs(flattenDriverValues(chunk)...).
				WillReturnResult(sqlmock.NewResult(0, int64(len(chunk))))
		}
		mock.ExpectClose()

		b.StartTimer()
		if err := upserter.Upsert(ctx, "users", columns, rows, uniqueKeys); err != nil {
			b.Fatalf("Upsert: %v", err)
		}
		b.StopTimer()

		if err := db.Close(); err != nil {
			b.Fatalf("db.Close: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			b.Fatalf("unmet expectations: %v", err)
		}
		b.StartTimer()
	}
}

func generateBenchmarkRows(count int) [][]any {
	rows := make([][]any, count)
	for i := 0; i < count; i++ {
		rows[i] = []any{int64(i + 1), fmt.Sprintf("name-%d", i+1)}
	}
	return rows
}

func driverArgs(row []any) []driver.Value {
	vals := make([]driver.Value, len(row))
	for i, v := range row {
		vals[i] = v
	}
	return vals
}

func flattenDriverValues(rows [][]any) []driver.Value {
	if len(rows) == 0 {
		return nil
	}
	flattened := make([]driver.Value, 0, len(rows)*len(rows[0]))
	for _, row := range rows {
		flattened = append(flattened, driverArgs(row)...)
	}
	return flattened
}
