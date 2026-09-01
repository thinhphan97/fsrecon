package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thinhphan97/fsrecon"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	var interval time.Duration
	if len(os.Args) > 2 {
		var err error
		interval, err = time.ParseDuration(os.Args[2])
		if err != nil {
			log.Fatalf("invalid reconcile interval: %v", err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: root, Recursive: true, ReconcileInterval: interval,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := tracker.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer tracker.Close()
	if interval == 0 {
		fmt.Printf("Watching %s with native filesystem events (periodic safety net disabled)\n", root)
	} else {
		fmt.Printf("Watching %s with native filesystem events (safety interval %s)\n", root, interval)
	}
	for event := range tracker.Events() {
		if event.OldPath != "" {
			fmt.Printf("%-18s %s -> %s\n", event.Kind, event.OldPath, event.Path)
			continue
		}
		fmt.Printf("%-18s %s\n", event.Kind, event.Path)
	}
}
