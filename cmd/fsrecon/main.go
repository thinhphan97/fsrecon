package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/thinhphan97/fsrecon"
)

func main() {
	if len(os.Args) != 3 {
		usage()
	}
	switch os.Args[1] {
	case "reconcile":
		reconcile(os.Args[2])
	case "watch":
		watch(os.Args[2])
	default:
		usage()
	}
}

func reconcile(root string) {
	tracker, err := fsrecon.New(fsrecon.Config{Root: root, Recursive: true})
	if err != nil {
		fail(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		fail(err)
	}
	for _, event := range report.Events {
		printEvent(event)
	}
	fmt.Printf("Scanned: %d  Healthy: %d  Missing: %d  Orphan: %d  Duration: %s\n",
		report.Scanned, report.Healthy, report.Missing, report.Orphan, report.Duration)
	if report.EventsTruncated > 0 {
		fmt.Printf("Event details truncated: %d\n", report.EventsTruncated)
	}
}

func watch(root string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tracker, err := fsrecon.New(fsrecon.Config{Root: root, Recursive: true})
	if err != nil {
		fail(err)
	}
	if err := tracker.Start(ctx); err != nil {
		fail(err)
	}
	defer tracker.Close()
	for {
		select {
		case event, ok := <-tracker.Events():
			if !ok {
				return
			}
			printEvent(event)
		case err, ok := <-tracker.Errors():
			if ok {
				fmt.Fprintln(os.Stderr, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func printEvent(event fsrecon.Event) {
	if event.OldPath != "" {
		fmt.Printf("%-18s %s -> %s\n", event.Kind, event.OldPath, event.Path)
		return
	}
	fmt.Printf("%-18s %s\n", event.Kind, event.Path)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fsrecon <watch|reconcile> ROOT")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
