package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/claude-code-go-rate-limiter/internal/httpapi"
	"example.com/claude-code-go-rate-limiter/internal/limiter"
)

func main() {
	store, err := limiter.NewStore(limiter.Config{
		Limit:           5,
		Window:          10 * time.Second,
		InactiveAfter:   time.Minute,
		CleanupInterval: 15 * time.Second,
	}, limiter.RealClock{})
	if err != nil {
		log.Fatalf("limiter config: %v", err)
	}
	store.Start()
	defer store.Close()

	mux := http.NewServeMux()
	mux.Handle("/", httpapi.NewHandler(store))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", server.Addr)
		errCh <- server.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("received %s; shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server: %v", err)
		}
	}
}
