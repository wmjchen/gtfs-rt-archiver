package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gtfs-rt-archiver/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.New(os.Stdout, os.Stderr).Run(ctx, os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
