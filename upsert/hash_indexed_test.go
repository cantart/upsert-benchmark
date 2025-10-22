package upsert

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHashIndexedUpserterUpsert_Batch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewHashIndexedUpserter(db)

	columns := []string{"id", "name", "email"}
	rows := [][]any{
		{int64(1), "John", "john@example.com"},
		{int64(2), "Jane", "jane@example.com"},
	}
	uniqueKeys := []string{"id"}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	expectedQuery := regexp.QuoteMeta(`INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "email" = EXCLUDED."email"`)

	mock.ExpectExec(expectedQuery).
		WithArgs(int64(1), "John", "john@example.com", int64(2), "Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(0, 2))

	if err := upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHashIndexedUpserterUpsert_DuplicateKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewHashIndexedUpserter(db)

	columns := []string{"id", "name"}
	rows := [][]any{
		{int64(1), "John"},
		{int64(1), "Jane"},
	}
	uniqueKeys := []string{"id"}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys)
	if err == nil {
		t.Fatal("expected duplicate keys error, got nil")
	}
	if mockErr := mock.ExpectationsWereMet(); mockErr != nil {
		t.Fatalf("unmet expectations: %v", mockErr)
	}
}

func TestHashIndexedUpserterUpsert_AllUniqueColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewHashIndexedUpserter(db)

	columns := []string{"id"}
	rows := [][]any{{int64(1)}}
	uniqueKeys := []string{"id"}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX IF NOT EXISTS "idx_de7ebd7b26552dfc" ON "users" ("id")`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	expectedQuery := regexp.QuoteMeta(`INSERT INTO "users" ("id") VALUES ($1) ON CONFLICT ("id") DO NOTHING`)

	mock.ExpectExec(expectedQuery).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
