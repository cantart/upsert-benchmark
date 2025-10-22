package upsert

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type HashIndexedUpserter struct {
	db *sql.DB
}

func NewHashIndexedUpserter(db *sql.DB) Upserter {
	return &HashIndexedUpserter{db: db}
}

func (h *HashIndexedUpserter) Upsert(ctx context.Context, table string, columns []string, rows [][]any, uniqueKeys []string) error {
	if len(columns) == 0 {
		return errors.New("at least one column is required")
	}
	if len(uniqueKeys) == 0 {
		return errors.New("at least one unique key is required")
	}
	if len(rows) == 0 {
		return nil
	}

	tableIdent, err := quoteIdentifier(table)
	if err != nil {
		return fmt.Errorf("table: %w", err)
	}

	columnIndex := make(map[string]int, len(columns))
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quoted, err := quoteIdentifier(col)
		if err != nil {
			return fmt.Errorf("column[%d]: %w", i, err)
		}
		quotedColumns[i] = quoted
		columnIndex[col] = i
	}

	quotedUniqueKeys := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		if _, ok := columnIndex[key]; !ok {
			return fmt.Errorf("unique key %q not found in columns", key)
		}
		quoted, err := quoteIdentifier(key)
		if err != nil {
			return fmt.Errorf("unique key %q: %w", key, err)
		}
		quotedUniqueKeys[i] = quoted
	}

	if err := h.ensureUniqueIndex(ctx, tableIdent, table, quotedUniqueKeys, uniqueKeys); err != nil {
		return err
	}

	seenKeys := make(map[string]int, len(rows))
	for idx, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("row %d: columns (%d) and values (%d) length mismatch", idx, len(columns), len(row))
		}
		key := compositeKey(row, uniqueKeys, columnIndex)
		if prev, ok := seenKeys[key]; ok {
			return fmt.Errorf("rows %d and %d share duplicate unique key values", prev, idx)
		}
		seenKeys[key] = idx
	}

	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	argIdx := 1
	for i, row := range rows {
		rowPlaceholders := make([]string, len(columns))
		for j := range columns {
			rowPlaceholders[j] = fmt.Sprintf("$%d", argIdx)
			args = append(args, row[j])
			argIdx++
		}
		placeholders[i] = fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", "))
	}

	setClauses := make([]string, 0, len(columns))
	uniqueSet := make(map[string]struct{}, len(uniqueKeys))
	for _, key := range uniqueKeys {
		uniqueSet[key] = struct{}{}
	}
	for i, col := range columns {
		if _, isUnique := uniqueSet[col]; isUnique {
			// Skip unique columns from SET clause to avoid redundant assignments.
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", quotedColumns[i], quotedColumns[i]))
	}

	var query string
	if len(setClauses) == 0 {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO NOTHING",
			tableIdent,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(quotedUniqueKeys, ", "),
		)
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			tableIdent,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(quotedUniqueKeys, ", "),
			strings.Join(setClauses, ", "),
		)
	}

	if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec upsert: %w", err)
	}
	return nil
}

func compositeKey(row []any, uniqueKeys []string, columnIndex map[string]int) string {
	var b strings.Builder
	for i, key := range uniqueKeys {
		if i > 0 {
			b.WriteString("|")
		}
		val := row[columnIndex[key]]
		b.WriteString(fmt.Sprintf("%v", val))
	}
	return b.String()
}

func (h *HashIndexedUpserter) ensureUniqueIndex(ctx context.Context, tableIdent string, rawTable string, quotedUniqueKeys []string, uniqueKeys []string) error {
	indexName := deriveIndexName(rawTable, uniqueKeys, "hash_idx")
	indexIdent, err := quoteIdentifier(indexName)
	if err != nil {
		return fmt.Errorf("index name: %w", err)
	}

	stmt := fmt.Sprintf(
		"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)",
		indexIdent,
		tableIdent,
		strings.Join(quotedUniqueKeys, ", "),
	)
	if _, err := h.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("create unique index: %w", err)
	}
	return nil
}
