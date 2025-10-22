package upsert

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBatchedHashIndexedUpserterUpsert_Chunks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewBatchedHashIndexedUpserter(db).(*BatchedHashIndexedUpserter).WithBatchSize(2)

	columns := []string{"id", "name"}
	rows := [][]any{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	}
	uniqueKeys := []string{"id"}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`INSERT INTO "users" \("id", "name"\) VALUES \(\$1, \$2\), \(\$3, \$4\) ON CONFLICT \("id"\) DO UPDATE SET "name" = EXCLUDED."name"`).
		WithArgs(1, "a", 2, "b").
		WillReturnResult(sqlmock.NewResult(0, 2))

	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`INSERT INTO "users" \("id", "name"\) VALUES \(\$1, \$2\) ON CONFLICT \("id"\) DO UPDATE SET "name" = EXCLUDED."name"`).
		WithArgs(3, "c").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchedHashIndexedUpserterUpsert_InvalidBatchSize(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	bad := &BatchedHashIndexedUpserter{db: db, batchSize: 0}

	if err := bad.Upsert(context.Background(), "users", []string{"id"}, [][]any{{1}}, []string{"id"}); err == nil {
		t.Fatal("expected error for non-positive batch size, got nil")
	}
}
