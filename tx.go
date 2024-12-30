package sqlite

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/trace"
	"time"
)

// BeginTx starts a new deferred [transaction].
// All queries in the connection before calling EndTx / RollbackTx are within the same transaction.
// The connection must not be used after the cleanup function was called.
//
//	ctx, tx, clean := p.BeginTx(ctx)
//	defer clean()
//
//	tx.Exec(ctx, …)
//	tx.EndTx(ctx)
//
// [transaction] https://www.sqlite.org/lang_transaction.html
func (p *Connections) BeginTx(ctx context.Context) (context.Context, *Conn, func()) {
	ctn := p.take()
	err := ctn.Exec(ctx, "BEGIN DEFERRED TRANSACTION").Err()
	if err != nil {
		// per documentation, this is not possible
		panic(fmt.Sprintf("assumption violation: the transactions could not be started (%s)", err))
	}

	ctn.zombie = time.AfterFunc(ZombieTimeout, func() {
		slog.Warn("zombie connection detected")
	})
	ctx, ctn.task = trace.NewTask(ctx, "db:sqlite-tx")
	return ctx, ctn, func() { p.put(ctn) }
}

// End terminates (commits) the current transaction.
// If the transaction is terminated (whether by commit or rollback), it is a no-op
func (ctn *Conn) EndTx(ctx context.Context) error {
	if err := ctn.Exec(ctx, "END TRANSACTION").Err(); err != nil {
		return err
	}

	ctn.zombie.Stop()
	ctn.task.End()
	return nil
}

// Rollback undoes changes to the current transaction.
// If the transaction is terminated (whether by commit or rollback), it is a no-op.
func (ctn *Conn) RollbackTx(ctx context.Context) error {
	if err := ctn.Exec(ctx, "ROLLBACK TRANSACTION").Err(); err != nil {
		return err
	}

	ctn.zombie.Stop()
	ctn.task.End()
	return nil
}
