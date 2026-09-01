package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thinhphan97/fsrecon"
)

type manifest []fsrecon.ExpectedEntry

func (entries manifest) WalkExpected(ctx context.Context, root string, emit func(fsrecon.ExpectedEntry) error) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(entry); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	expectedSize := int64(4)
	tracker, err := fsrecon.New(fsrecon.Config{
		Root: ".", Recursive: true,
		Expected: manifest{{Path: "data.bin", Size: &expectedSize}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tracker.Close()
	report, err := tracker.Reconcile(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, event := range report.Events {
		if event.Kind == fsrecon.EventMissing || event.Kind == fsrecon.EventInvalid {
			fmt.Println(event.Kind, event.Path)
		}
	}
}
