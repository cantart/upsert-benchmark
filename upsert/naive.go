package upsert

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type NaiveUpserter struct {
	db *sql.DB
}

func NewNaiveUpserter(db *sql.DB) Upserter {
	return &NaiveUpserter{db: db}
}

func (n *NaiveUpserter) Upsert(ctx context.Context, table string, columns []string, rows [][]any, uniqueKeys []string) error {
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

	whereClauses := make([]string, len(uniqueKeys))
	quotedUniqueKeys := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		if _, ok := columnIndex[key]; !ok {
			return fmt.Errorf("unique key %q not found in columns", key)
		}
		quotedKey, err := quoteIdentifier(key)
		if err != nil {
			return fmt.Errorf("unique key %q: %w", key, err)
		}
		quotedUniqueKeys[i] = quotedKey
		whereClauses[i] = fmt.Sprintf("%s = $%d", quotedKey, i+1)
	}

	tx, err := n.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	checkQuery := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1", tableIdent, strings.Join(whereClauses, " AND "))
	for rowIdx, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("row %d: columns (%d) and values (%d) length mismatch", rowIdx, len(columns), len(row))
		}

		whereArgs := make([]any, len(uniqueKeys))
		for i, key := range uniqueKeys {
			whereArgs[i] = row[columnIndex[key]]
		}

		var one int
		exists := false
		switch err := tx.QueryRowContext(ctx, checkQuery, whereArgs...).Scan(&one); {
		case errors.Is(err, sql.ErrNoRows):
			exists = false
		case err != nil:
			return fmt.Errorf("row %d: check existing row: %w", rowIdx, err)
		default:
			exists = true
		}

		if exists {
			if err := n.executeUpdate(ctx, tx, tableIdent, quotedColumns, columns, columnIndex, row, uniqueKeys, quotedUniqueKeys); err != nil {
				return fmt.Errorf("row %d: %w", rowIdx, err)
			}
		} else {
			if err := n.executeInsert(ctx, tx, tableIdent, quotedColumns, row); err != nil {
				return fmt.Errorf("row %d: %w", rowIdx, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return nil
}

func (n *NaiveUpserter) executeInsert(ctx context.Context, tx *sql.Tx, table string, quotedColumns []string, row []any) error {
	placeholders := make([]string, len(row))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(quotedColumns, ", "), strings.Join(placeholders, ", "))
	if _, err := tx.ExecContext(ctx, insertQuery, row...); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}
	return nil
}

func (n *NaiveUpserter) executeUpdate(ctx context.Context, tx *sql.Tx, table string, quotedColumns []string, columns []string, columnIndex map[string]int, row []any, uniqueKeys []string, quotedUniqueKeys []string) error {
	setClauses := make([]string, len(columns))
	args := make([]any, 0, len(columns)+len(uniqueKeys))
	idx := 1
	for i := range columns {
		setClauses[i] = fmt.Sprintf("%s = $%d", quotedColumns[i], idx)
		args = append(args, row[i])
		idx++
	}

	whereClauses := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		quotedKey := quotedUniqueKeys[i]
		if quotedKey == "" {
			return fmt.Errorf("unique key %q: missing quoted identifier", key)
		}
		whereClauses[i] = fmt.Sprintf("%s = $%d", quotedKey, idx)
		args = append(args, row[columnIndex[key]])
		idx++
	}

	updateQuery := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(setClauses, ", "), strings.Join(whereClauses, " AND "))
	if _, err := tx.ExecContext(ctx, updateQuery, args...); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	return nil
}
