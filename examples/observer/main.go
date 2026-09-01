package main

import (
	"context"
	"log"
	"os"

	fsrecon "github.com/thinhphan97/fsrecon"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	o, err := fsrecon.NewObserver(fsrecon.ObserverConfig{Root: root, Recursive: true})
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := o.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer o.Close()
	for hint := range o.Hints() {
		log.Printf("filesystem invalidation: path=%s scope=%s cause=%s", hint.Path, hint.Scope, hint.Cause)
	}
}
