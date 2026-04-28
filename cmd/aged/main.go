package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"aged/internal/eventstore"
	"aged/internal/httpapi"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func main() {
	var (
		addr        = flag.String("addr", envOr("AGED_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		dbPath      = flag.String("db", envOr("AGED_DB", "aged.db"), "SQLite database path")
		workDir     = flag.String("workdir", envOr("AGED_WORKDIR", "."), "worker working directory")
		workerKind  = flag.String("worker", envOr("AGED_DEFAULT_WORKER", "mock"), "default worker kind")
		promptPath  = flag.String("prompt", envOr("AGED_ORCHESTRATOR_PROMPT", "prompts/orchestrator.md"), "orchestrator prompt template")
		webDistPath = flag.String("web", envOr("AGED_WEB_DIST", "web/dist"), "built web dashboard directory")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := eventstore.OpenSQLite(ctx, *dbPath)
	if err != nil {
		slog.Error("open event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	var brain orchestrator.BrainProvider
	brain, err = orchestrator.NewPromptBrain(*workerKind, *promptPath)
	if err != nil {
		slog.Warn("using static brain because prompt template could not be loaded", "error", err)
		brain = &orchestrator.StaticBrain{WorkerKind: *workerKind}
	}

	absWorkDir, err := filepath.Abs(*workDir)
	if err != nil {
		slog.Error("resolve workdir", "error", err)
		os.Exit(1)
	}

	service := orchestrator.NewService(store, brain, worker.DefaultRunners(), absWorkDir)
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(service, staticHandler(*webDistPath)).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("aged listening", "addr", "http://"+*addr, "db", *dbPath, "workdir", absWorkDir)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "error", err)
	}
}

func envOr(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func staticHandler(path string) http.Handler {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return http.FileServer(http.Dir(path))
}
