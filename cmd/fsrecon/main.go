package main

import (
	"context"
	"fmt"
	"os"

	"github.com/thinhphan97/fsrecon"
)

func main() {
	if len(os.Args) != 3 || os.Args[1] != "reconcile" {
		fmt.Fprintln(os.Stderr, "usage: fsrecon reconcile ROOT")
		os.Exit(2)
	}
	tracker, err := fsrecon.New(fsrecon.Config{Root: os.Args[2], Recursive: true})
	if err != nil {
		fail(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		fail(err)
	}
	for _, event := range report.Events {
		fmt.Printf("%-18s %s\n", event.Kind, event.Path)
	}
	fmt.Printf("Scanned: %d  Healthy: %d  Missing: %d  Orphan: %d  Duration: %s\n",
		report.Scanned, report.Healthy, report.Missing, report.Orphan, report.Duration)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
