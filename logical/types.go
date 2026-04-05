package logical

import "github.com/pg2iceberg/pg2iceberg/postgres"

// Re-export shared types from the source package so that code within the
// logical package can reference them without a package qualifier. This avoids
// modifying every reference in the files moved from package postgres.
type ChangeEvent = postgres.ChangeEvent
type SchemaChange = postgres.SchemaChange
type SchemaColumn = postgres.SchemaColumn
type TypeChange = postgres.TypeChange
type Op = postgres.Op

const (
	OpInsert              = postgres.OpInsert
	OpUpdate              = postgres.OpUpdate
	OpDelete              = postgres.OpDelete
	OpSnapshotTableComplete = postgres.OpSnapshotTableComplete
	OpSnapshotComplete    = postgres.OpSnapshotComplete
	OpBegin               = postgres.OpBegin
	OpCommit              = postgres.OpCommit
	OpSchemaChange        = postgres.OpSchemaChange
)
