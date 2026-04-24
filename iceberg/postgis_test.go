package iceberg

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

func TestToPostGISBytes_DecodesHexEWKB(t *testing.T) {
	// POINT(1 1) with SRID 4326, little-endian EWKB.
	hexEWKB := "0101000020E6100000000000000000F03F000000000000F03F"
	want, _ := hex.DecodeString(hexEWKB)

	got, err := ToPostGISBytes(hexEWKB)
	if err != nil {
		t.Fatalf("ToPostGISBytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded bytes mismatch\n  got:  %x\n  want: %x", got, want)
	}
}

func TestToPostGISBytes_PassesThroughRawBytes(t *testing.T) {
	in := []byte{0x01, 0x02, 0x03, 0x04}
	got, err := ToPostGISBytes(in)
	if err != nil {
		t.Fatalf("ToPostGISBytes: %v", err)
	}
	if !bytes.Equal(got, in) {
		t.Fatalf("got %x, want %x", got, in)
	}
	// Must be a copy, not the same slice — writer retains the builder's ownership.
	in[0] = 0xFF
	if got[0] == 0xFF {
		t.Fatalf("ToPostGISBytes must copy input []byte, not alias it")
	}
}

func TestToPostGISBytes_EmptyString(t *testing.T) {
	got, err := ToPostGISBytes("")
	if err != nil {
		t.Fatalf("ToPostGISBytes(empty): %v", err)
	}
	if got != nil {
		t.Fatalf("want nil bytes for empty string, got %x", got)
	}
}

func TestToPostGISBytes_RejectsNonHex(t *testing.T) {
	if _, err := ToPostGISBytes("not-hex!!"); err == nil {
		t.Fatalf("expected error for non-hex input")
	}
}

// TestParquetWriter_GeometryRoundtrip verifies that a hex EWKB value written
// through the parquet writer arrives as raw bytes (not a string) and retains
// the exact byte content.
func TestParquetWriter_GeometryRoundtrip(t *testing.T) {
	ts := &postgres.TableSchema{
		Table: "public.places",
		Columns: []postgres.Column{
			{Name: "id", PGType: postgres.Int4, IsNullable: false, FieldID: 1},
			{Name: "geom", PGType: postgres.Geometry, IsNullable: true, FieldID: 2, SRID: 4326},
		},
		PK: []string{"id"},
	}

	w := NewDataWriter(ts)
	hexEWKB := "0101000020E6100000000000000000F03F000000000000F03F"
	wantBytes, _ := hex.DecodeString(hexEWKB)

	if err := w.Add(map[string]any{"id": "1", "geom": hexEWKB}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := w.Add(map[string]any{"id": "2", "geom": nil}); err != nil {
		t.Fatalf("Add nil: %v", err)
	}

	data, count, err := w.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2", count)
	}
	if len(data) == 0 {
		t.Fatalf("expected non-empty parquet output")
	}

	// Verify the bytes we want to round-trip are actually present in the
	// parquet file. Snappy is the compression codec so the raw EWKB should
	// live in a data page uncompressed-short-enough to appear verbatim.
	if !bytes.Contains(data, wantBytes) {
		t.Fatalf("parquet output does not contain expected EWKB bytes %x", wantBytes)
	}
}
