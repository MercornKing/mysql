package mysql

import (
	"context"
	"database/sql"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Helper to verify that all connections in the pool have @@in_transaction = 0
func verifyPoolCleanliness(t *testing.T, db *sql.DB, numChecks int) {
	t.Helper()
	for i := 0; i < numChecks; i++ {
		var inTx int
		err := db.QueryRow("SELECT @@in_transaction").Scan(&inTx)
		if err != nil {
			t.Fatalf("check %d failed to query @@in_transaction: %v", i, err)
		}
		if inTx != 0 {
			t.Fatalf("check %d pool pollution detected: @@in_transaction = %d, expected 0", i, inTx)
		}
	}
}

// Requirement 2: Test calling BeginTx with a pre-canceled context.
func TestBeginTx_PreCanceledContext(t *testing.T) {
	DefaultDriver.Reset()
	db, err := sql.Open("mock_mysql", "test_precancel")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	tx, err := db.BeginTx(ctx, nil)
	if err == nil {
		tx.Rollback()
		t.Fatal("expected error calling BeginTx with pre-canceled context, got nil")
	}

	if err != context.Canceled {
		t.Logf("BeginTx returned error: %v (expected context.Canceled)", err)
	}

	// Requirement 3: Verify pool cleanliness by checking @@in_transaction
	verifyPoolCleanliness(t, db, 5)
}

// Requirement 1: Test simulating context cancellation during transaction start.
func TestBeginTx_ContextCanceledDuringStart(t *testing.T) {
	DefaultDriver.Reset()
	DefaultDriver.SetTxStartDelay(50 * time.Millisecond)

	db, err := sql.Open("mock_mysql", "test_cancel_during_start")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err == nil {
		tx.Rollback()
		t.Fatal("expected error due to context timeout during BeginTx, got nil")
	}

	// Verify pool cleanliness by checking @@in_transaction
	verifyPoolCleanliness(t, db, 5)
}

// Acceptance Criteria 2: Test rollback or closing connection when state is uncertain.
func TestBeginTx_UncertainState_ClosesConnection(t *testing.T) {
	DefaultDriver.Reset()
	DefaultDriver.SetTxStartDelay(30 * time.Millisecond)
	DefaultDriver.SetFailRollback(true)

	db, err := sql.Open("mock_mysql", "test_uncertain_state")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err == nil {
		tx.Rollback()
		t.Fatal("expected error due to failed rollback/uncertain state, got nil")
	}

	// Connection should have been closed/discarded by driver, preventing active tx leak
	verifyPoolCleanliness(t, db, 5)
}

// Acceptance Criteria 3: Verify pool cleanliness under high concurrency and frequent timeouts.
func TestPoolCleanliness_HighConcurrency(t *testing.T) {
	DefaultDriver.Reset()
	DefaultDriver.SetTxStartDelay(5 * time.Millisecond)

	db, err := sql.Open("mock_mysql", "test_high_concurrency")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	const numWorkers = 50
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()

			// Random timeout: some will time out during start, some will succeed, some pre-canceled
			timeout := time.Duration(rand.Intn(12)) * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			tx, err := db.BeginTx(ctx, nil)
			if err == nil {
				// Simulate brief work inside transaction
				time.Sleep(2 * time.Millisecond)
				if id%2 == 0 {
					_ = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			}
		}(i)
	}

	wg.Wait()

	// Requirement 3: Verify pool cleanliness across all connections
	verifyPoolCleanliness(t, db, 20)
}

// Acceptance Criteria 4: Verify no race conditions between query execution and cancellation listener.
func TestNoRaceConditions(t *testing.T) {
	DefaultDriver.Reset()
	DefaultDriver.SetTxStartDelay(2 * time.Millisecond)

	db, err := sql.Open("mock_mysql", "test_race_conditions")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
			defer cancel()

			tx, err := db.BeginTx(ctx, nil)
			if err == nil {
				_ = tx.Rollback()
			}
		}()
	}

	wg.Wait()
	verifyPoolCleanliness(t, db, 10)
}
