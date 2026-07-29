package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/url"
	"sync"
	"time"
)

var DefaultDriver = &MockDriver{}

func init() {
	sql.Register("mock_mysql", DefaultDriver)
}

// MockDriver implements driver.Driver for mock MySQL testing.
type MockDriver struct {
	mu           sync.Mutex
	txStartDelay time.Duration
	failRollback bool
	conns        []*MockConn
}

func (d *MockDriver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	delay := d.txStartDelay
	failRB := d.failRollback
	d.mu.Unlock()

	if u, err := url.Parse(name); err == nil {
		q := u.Query()
		if dStr := q.Get("delay"); dStr != "" {
			if parsed, err := time.ParseDuration(dStr); err == nil {
				delay = parsed
			}
		}
		if q.Get("fail_rollback") == "true" {
			failRB = true
		}
	}

	conn := &MockConn{
		txStartDelay: delay,
		failRollback: failRB,
		driver:       d,
	}

	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()

	return conn, nil
}

func (d *MockDriver) SetTxStartDelay(delay time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.txStartDelay = delay
}

func (d *MockDriver) SetFailRollback(fail bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failRollback = fail
}

func (d *MockDriver) GetConns() []*MockConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]*MockConn, len(d.conns))
	copy(cp, d.conns)
	return cp
}

func (d *MockDriver) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.txStartDelay = 0
	d.failRollback = false
	d.conns = nil
}

// MockConn represents a mock MySQL database connection.
type MockConn struct {
	mu           sync.Mutex
	closed       bool
	inTx         bool
	txStartDelay time.Duration
	failRollback bool
	driver       *MockDriver
}

func (mc *MockConn) Prepare(query string) (driver.Stmt, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return nil, driver.ErrBadConn
	}
	return &MockStmt{conn: mc, query: query}, nil
}

func (mc *MockConn) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.closed = true
	return nil
}

func (mc *MockConn) Begin() (driver.Tx, error) {
	return mc.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with context awareness, rollback handling, and pool protection.
func (mc *MockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// 1. Check pre-canceled context
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mc.mu.Lock()
	if mc.closed {
		mc.mu.Unlock()
		return nil, driver.ErrBadConn
	}
	delay := mc.txStartDelay
	mc.mu.Unlock()

	// 2. Simulate transaction start command ("START TRANSACTION")
	done := make(chan error, 1)

	go func() {
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
		}

		mc.mu.Lock()
		defer mc.mu.Unlock()

		if mc.closed {
			done <- driver.ErrBadConn
			return
		}

		mc.inTx = true
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			return nil, err
		}

		// Context canceled right after START TRANSACTION completed
		if err := ctx.Err(); err != nil {
			return nil, mc.cleanupCanceledTx(err)
		}

		return &MockTx{conn: mc}, nil

	case <-ctx.Done():
		// Wait for initialization goroutine to finish updating connection state
		<-done

		mc.mu.Lock()
		inTx := mc.inTx
		mc.mu.Unlock()

		if !inTx {
			// Transaction was never started on server/connection
			return nil, ctx.Err()
		}

		// Transaction was started; attempt rollback or discard connection
		return nil, mc.cleanupCanceledTx(ctx.Err())
	}
}

// cleanupCanceledTx rolls back an active transaction or closes the connection if state is uncertain.
func (mc *MockConn) cleanupCanceledTx(ctxErr error) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return driver.ErrBadConn
	}

	// If rollback fails or connection state is uncertain, discard connection completely
	if mc.failRollback {
		mc.closed = true
		return driver.ErrBadConn
	}

	// Successfully execute ROLLBACK to clean connection state
	mc.inTx = false
	return ctxErr
}

func (mc *MockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return nil, driver.ErrBadConn
	}

	switch query {
	case "START TRANSACTION", "BEGIN":
		mc.inTx = true
	case "COMMIT", "ROLLBACK":
		mc.inTx = false
	}
	return driver.RowsAffected(1), nil
}

func (mc *MockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return nil, driver.ErrBadConn
	}

	if query == "SELECT @@in_transaction" || query == "SELECT @@in_transaction AS in_tx" {
		inTxVal := int64(0)
		if mc.inTx {
			inTxVal = 1
		}
		return &MockRows{
			columns: []string{"@@in_transaction"},
			values:  [][]driver.Value{{inTxVal}},
		}, nil
	}

	return &MockRows{
		columns: []string{"result"},
		values:  [][]driver.Value{{"ok"}},
	}, nil
}

func (mc *MockConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return driver.ErrBadConn
	}
	return nil
}

func (mc *MockConn) IsValid() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return !mc.closed
}

func (mc *MockConn) InTx() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.inTx
}

func (mc *MockConn) IsClosed() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.closed
}

// MockTx implements driver.Tx.
type MockTx struct {
	conn *MockConn
	done bool
}

func (tx *MockTx) Commit() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if tx.done {
		return sql.ErrTxDone
	}
	tx.done = true
	if tx.conn.closed {
		return driver.ErrBadConn
	}
	if !tx.conn.inTx {
		return errors.New("no active transaction")
	}
	tx.conn.inTx = false
	return nil
}

func (tx *MockTx) Rollback() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if tx.done {
		return sql.ErrTxDone
	}
	tx.done = true
	if tx.conn.closed {
		return driver.ErrBadConn
	}
	if !tx.conn.inTx {
		return errors.New("no active transaction")
	}
	tx.conn.inTx = false
	return nil
}

// MockStmt implements driver.Stmt.
type MockStmt struct {
	conn  *MockConn
	query string
}

func (s *MockStmt) Close() error { return nil }
func (s *MockStmt) NumInput() int { return -1 }

func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, nil)
}

func (s *MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, nil)
}

// MockRows implements driver.Rows.
type MockRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (mr *MockRows) Columns() []string {
	return mr.columns
}

func (mr *MockRows) Close() error {
	return nil
}

func (mr *MockRows) Next(dest []driver.Value) error {
	if mr.index >= len(mr.values) {
		return io.EOF
	}
	row := mr.values[mr.index]
	mr.index++
	for i, val := range row {
		dest[i] = val
	}
	return nil
}
