package lib

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// FormatTable writes rows as an aligned text table to w.
// headers defines column names. rows is a slice of slices where each inner
// slice corresponds to one row's cell values (pre-formatted as strings).
func FormatTable(w io.Writer, headers []string, rows [][]string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	fmt.Fprintln(tw, strings.Repeat("-\t", len(headers)))
	for _, row := range rows {
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	tw.Flush()
}

// FormatJSON marshals v as indented JSON and writes it to w.
func FormatJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// FormatMillis converts an epoch-millisecond timestamp to a human-readable
// string. Returns "-" for zero values (no timestamp set).
func FormatMillis(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
