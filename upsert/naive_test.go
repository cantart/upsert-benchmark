package upsert

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNaiveUpserterUpsert_MultiRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewNaiveUpserter(db)

	columns := []string{"id", "name", "email"}
	rows := [][]any{
		{int64(1), "John", "john@example.com"},
		{int64(2), "Jane", "jane@example.com"},
	}
	uniqueKeys := []string{"id"}

	mock.ExpectBegin()

	mock.ExpectQuery(`SELECT 1 FROM "users" WHERE "id" = \$1 LIMIT 1`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))

	mock.ExpectExec(`UPDATE "users" SET "id" = \$1, "name" = \$2, "email" = \$3 WHERE "id" = \$4`).
		WithArgs(int64(1), "John", "john@example.com", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(`SELECT 1 FROM "users" WHERE "id" = \$1 LIMIT 1`).
		WithArgs(int64(2)).
		WillReturnError(sql.ErrNoRows)

	mock.ExpectExec(`INSERT INTO "users" \("id", "name", "email"\) VALUES \(\$1, \$2, \$3\)`).
		WithArgs(int64(2), "Jane", "jane@example.com").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	if err := upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestNaiveUpserterUpsert_RowLengthMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	upserter := NewNaiveUpserter(db)

	columns := []string{"id", "name", "email"}
	rows := [][]any{
		{int64(1), "John"},
	}
	uniqueKeys := []string{"id"}

	mock.ExpectBegin()
	mock.ExpectRollback()

	err = upserter.Upsert(context.Background(), "users", columns, rows, uniqueKeys)
	if err == nil {
		t.Fatal("expected error for row length mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "row 0: columns (3) and values (2)") {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
