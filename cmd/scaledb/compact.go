package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/scaledb-io/scaledb/pkg/compact"
)

// runCompact implements the `scaledb compact <path>` subcommand.
func runCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)

	targetSizeStr := fs.String("target-size", "512MB",
		"Target output file size (e.g. 128MB, 512MB, 1GB)")
	olderThanStr := fs.String("older-than", "7d",
		"Only compact partitions older than this duration (e.g. 3d, 24h)")
	dataTypesStr := fs.String("datatypes", "",
		"Comma-separated list of data types to compact (default: all)")
	dryRun := fs.Bool("dry-run", false,
		"Report what would be compacted without modifying files")

	_ = fs.Parse(args) // ExitOnError means this never returns an error, but errcheck requires handling

	dataPath := fs.Arg(0)
	if dataPath == "" {
		fmt.Fprintf(os.Stderr, "Error: data path argument is required\n")
		fmt.Fprintf(os.Stderr, "Usage: scaledb compact <path> [flags]\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	targetSize, err := parseBytes(*targetSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid --target-size %q: %v\n", *targetSizeStr, err)
		os.Exit(1)
	}

	olderThan, err := parseDuration(*olderThanStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid --older-than %q: %v\n", *olderThanStr, err)
		os.Exit(1)
	}

	var dataTypes []string
	if *dataTypesStr != "" {
		for _, dt := range strings.Split(*dataTypesStr, ",") {
			dt = strings.TrimSpace(dt)
			if dt != "" {
				dataTypes = append(dataTypes, dt)
			}
		}
	}

	opts := compact.Options{
		TargetSize: targetSize,
		OlderThan:  olderThan,
		DataTypes:  dataTypes,
		DryRun:     *dryRun,
	}

	if *dryRun {
		fmt.Println("=== DRY RUN — no files will be modified ===")
	}
	fmt.Printf("Compacting %s  (target-size=%s  older-than=%s)\n\n",
		dataPath, *targetSizeStr, *olderThanStr)

	report, err := compact.Compact(context.Background(), dataPath, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	printCompactReport(report)
}

func printCompactReport(r *compact.Report) {
	for _, p := range r.Partitions {
		if p.Skipped {
			fmt.Printf("  SKIP  %s/instance_id=%s/date=%s  (%s)\n",
				p.DataType, p.InstanceID, p.Date, p.SkipReason)
			continue
		}
		fmt.Printf("  OK    %s/instance_id=%s/date=%s  %d→%d files  %s→%s\n",
			p.DataType, p.InstanceID, p.Date,
			p.InputFiles, p.OutputFiles,
			humanBytes(p.InputBytes), humanBytes(p.OutputBytes))
	}

	fmt.Println()
	if r.DryRun {
		fmt.Printf("DRY RUN summary: %d partitions would be compacted, %d skipped\n",
			len(r.Partitions)-r.PartSkipped, r.PartSkipped)
	} else {
		fmt.Printf("Done: %d files → %d files  (%s → %s)  %d partitions skipped\n",
			r.FilesIn, r.FilesOut,
			humanBytes(r.TotalIn), humanBytes(r.TotalOut),
			r.PartSkipped)
	}
}

// parseBytes parses a human-readable byte size (e.g. "512MB", "1GB").
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	units := map[string]int64{
		"B":   1,
		"KB":  1024,
		"MB":  1024 * 1024,
		"GB":  1024 * 1024 * 1024,
		"TB":  1024 * 1024 * 1024 * 1024,
		"KIB": 1024,
		"MIB": 1024 * 1024,
		"GIB": 1024 * 1024 * 1024,
	}
	for suffix, mul := range units {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSuffix(s, suffix)
			var n float64
			if _, err := fmt.Sscanf(numStr, "%f", &n); err != nil {
				return 0, fmt.Errorf("cannot parse %q", s)
			}
			return int64(n * float64(mul)), nil
		}
	}
	// Try raw integer.
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("cannot parse %q — use e.g. 512MB, 1GB", s)
	}
	return n, nil
}

// parseDuration extends time.ParseDuration with day units (e.g. "7d", "30d").
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		var n float64
		if _, err := fmt.Sscanf(numStr, "%f", &n); err != nil {
			return 0, fmt.Errorf("cannot parse %q", s)
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(s)
}
