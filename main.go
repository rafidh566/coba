package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const (
	totalRecords = 10000000
	batchSize    = 1000
)

func randomTimestamp() time.Time {
	// Generate a random timestamp between Jan 1, 2018 and now.
	start := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	delta := time.Since(start)
	offset := time.Duration(rand.Int63n(int64(delta)))
	return start.Add(offset)
}

func randomAmount() float64 {
	// Generate a random amount between 1.00 and 10,000.00
	return float64(rand.Intn(1000000)+100) / 100.0
}

func randomTransactionType() string {
	types := []string{"debit", "credit"}
	return types[rand.Intn(len(types))]
}

func randomDescription() string {
	descriptions := []string{
		"Payment for invoice",
		"Refund processed",
		"Subscription fee",
		"Transfer to savings",
		"Purchase at store",
		"Online order",
		"ATM withdrawal",
		"Salary deposit",
		"Bill payment",
		"Cash deposit",
	}
	return descriptions[rand.Intn(len(descriptions))]
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Replace with your actual Supabase/PostgreSQL credentials.
	// For Supabase, you can find these in your project settings.
	connStr := "postgres://supabase:Ugierafie566@?@localhost:5432/postgres?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening connection: %v", err)
	}
	defer db.Close()

	startTime := time.Now()
	recordsInserted := 0

	for i := 0; i < totalRecords; i += batchSize {
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Error beginning transaction: %v", err)
		}

		// Use the fully qualified table name "public.transaksi"
		stmt, err := tx.Prepare(pq.CopyIn("public.transaksi",
			"account_id",
			"transaksi_date",
			"amount",
			"transaction_type",
			"description",
			"created_at",
			"updated_at",
		))
		if err != nil {
			log.Fatalf("Error preparing COPY statement: %v", err)
		}

		for j := 0; j < batchSize && i+j < totalRecords; j++ {
			// Assuming account_id is between 1 and 100,000.
			accountID := rand.Intn(100000) + 1
			ts := randomTimestamp()
			amount := randomAmount()
			transactionType := randomTransactionType()
			description := randomDescription()

			// Using the same timestamp for created_at and updated_at.
			_, err = stmt.Exec(accountID, ts, amount, transactionType, description, ts, ts)
			if err != nil {
				log.Fatalf("Error executing COPY row insert: %v", err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			log.Fatalf("Error finalizing COPY for batch: %v", err)
		}

		err = stmt.Close()
		if err != nil {
			log.Fatalf("Error closing COPY statement: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			log.Fatalf("Error committing transaction: %v", err)
		}

		recordsInserted += batchSize
		if recordsInserted%10000 == 0 {
			fmt.Printf("Inserted %d records so far...\n", recordsInserted)
		}
	}

	fmt.Printf("Finished inserting %d records in %s\n", totalRecords, time.Since(startTime))
}
