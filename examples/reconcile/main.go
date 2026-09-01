package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thinhphan97/fsrecon"
)

func main() {
	tracker, err := fsrecon.New(fsrecon.Config{Root: ".", Recursive: true})
	if err != nil {
		log.Fatal(err)
	}
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, event := range report.Events {
		fmt.Println(event.Kind, event.Path)
	}
}
