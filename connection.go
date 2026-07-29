// Proposed fix for raimeecas/mysql issue #2 (Prevent Connection Leak to Pool on Context Cancellation during BeginTx())
// This should be applied in the mysql driver's transaction initialization flow, likely in connection.go or transaction.go

package mysql

import (
	"context"
)

// Example integration of the fix into the BeginTx method:
/*
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finishCancel()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Send BEGIN command to MySQL
	err := mc.writeCommandPacketStr(comQuery, "START TRANSACTION")
	if err != nil {
		return nil, err
	}

	// Read the result
	_, err = mc.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	// CRITICAL FIX: Check if context was canceled during the roundtrip
	select {
	case <-ctx.Done():
		// The transaction started on the server, but the context is dead.
		// We must close the connection to prevent returning a dirty connection to the pool.
		mc.Close() 
		return nil, ctx.Err()
	default:
		// Proceed normally
	}

	return &mysqlTx{mc}, nil
}
*/
