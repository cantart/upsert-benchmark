package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
		"localhost", 5432, "postgres", "upsertbenchmark", "postgres")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("open postgres: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}

	// Example of inserting/updating a record
	stmt := `INSERT INTO users (id, name, email) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET name = $2, email = $3`
	if _, err := db.Exec(stmt, 2, "John Doe", "john@example.com"); err != nil {
		log.Printf("upsert user: %v", err)
	} else {
		log.Println("upserted demo user")
	}

	// Example of querying a single row
	var name, email string
	switch err := db.QueryRow("SELECT name, email FROM users WHERE id = $1", 1).Scan(&name, &email); {
	case err == sql.ErrNoRows:
		log.Println("no user found with ID 1")
	case err != nil:
		log.Printf("query user by id: %v", err)
	default:
		log.Printf("loaded user: %s <%s>", name, email)
	}

	// Example of querying multiple rows
	rows, err := db.Query("SELECT id, name FROM users ORDER BY id LIMIT 5")
	if err != nil {
		log.Printf("query users: %v", err)
	} else {
		defer rows.Close()

		log.Println("first five users:")
		for rows.Next() {
			var id int64
			var rowName string
			if err := rows.Scan(&id, &rowName); err != nil {
				log.Printf("scan user row: %v", err)
				break
			}
			log.Printf("- %d: %s", id, rowName)
		}
		if err := rows.Err(); err != nil {
			log.Printf("iterating user rows: %v", err)
		}
	}

	// Example of using a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("begin transaction: %v", err)
	} else {
		committed := false
		defer func() {
			if !committed {
				if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
					log.Printf("transaction rollback: %v", err)
				}
			}
		}()

		if _, err := tx.Exec("UPDATE users SET name = $1 WHERE id = $2", "Jane Doe", 1); err != nil {
			log.Printf("update user in transaction: %v", err)
		} else if err := tx.Commit(); err != nil {
			log.Printf("commit transaction: %v", err)
		} else {
			committed = true
			log.Println("transaction committed")
		}
	}
}
