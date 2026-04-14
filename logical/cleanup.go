package logical

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// Cleanup drops the replication slot, publication, and coordinator schema
// created by a pg2iceberg pipeline. It waits for the slot to become inactive
// before dropping it (up to 60 seconds).
func Cleanup(ctx context.Context, conn *pgx.Conn, slotName, publicationName, coordinatorSchema string) error {
	// Wait for slot to become inactive (pipeline may still be shutting down).
	for i := 0; i < 30; i++ {
		var active bool
		err := conn.QueryRow(ctx,
			"SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName,
		).Scan(&active)
		if err != nil {
			// Slot doesn't exist — nothing to drop.
			log.Printf("[cleanup] slot %q not found, skipping", slotName)
			goto dropPublication
		}
		if !active {
			break
		}
		log.Printf("[cleanup] slot %q still active, waiting...", slotName)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	// Drop replication slot.
	if _, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName); err != nil {
		log.Printf("[cleanup] drop slot %q: %v", slotName, err)
	} else {
		log.Printf("[cleanup] dropped slot %q", slotName)
	}

dropPublication:
	// Drop publication.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{publicationName}.Sanitize())); err != nil {
		log.Printf("[cleanup] drop publication %q: %v", publicationName, err)
	} else {
		log.Printf("[cleanup] dropped publication %q", publicationName)
	}

	// Drop coordinator schema.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgx.Identifier{coordinatorSchema}.Sanitize())); err != nil {
		log.Printf("[cleanup] drop schema %q: %v", coordinatorSchema, err)
	} else {
		log.Printf("[cleanup] dropped schema %q", coordinatorSchema)
	}

	log.Println("[cleanup] done")
	return nil
}
