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
		addr             = flag.String("addr", envOr("AGED_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		dbPath           = flag.String("db", envOr("AGED_DB", "aged.db"), "SQLite database path")
		workDir          = flag.String("workdir", envOr("AGED_WORKDIR", "."), "worker working directory")
		workerKind       = flag.String("worker", envOr("AGED_DEFAULT_WORKER", "mock"), "orchestrator fallback worker kind")
		brainMode        = flag.String("brain", envOr("AGED_BRAIN", "prompt"), "brain provider: prompt, codex, api, or static")
		promptPath       = flag.String("prompt", envOr("AGED_ORCHESTRATOR_PROMPT", "prompts/orchestrator.md"), "fallback worker prompt template")
		schedulerPrompt  = flag.String("scheduler-prompt", envOr("AGED_SCHEDULER_PROMPT", "prompts/scheduler.md"), "API scheduler prompt template")
		brainEndpoint    = flag.String("brain-endpoint", envOr("AGED_BRAIN_ENDPOINT", "https://api.openai.com/v1/chat/completions"), "OpenAI-compatible chat completions endpoint")
		brainAPIKey      = flag.String("brain-api-key", envFirst("AGED_BRAIN_API_KEY", "OPENAI_API_KEY"), "API key for the API brain provider")
		brainModel       = flag.String("brain-model", envOr("AGED_BRAIN_MODEL", ""), "model for the API brain provider")
		codexPath        = flag.String("codex-path", envOr("AGED_CODEX_PATH", "codex"), "Codex CLI path for the codex brain")
		workspaceVCS     = flag.String("workspace-vcs", envOr("AGED_WORKSPACE_VCS", "auto"), "worker workspace VCS: auto, jj, or git")
		workspaceMode    = flag.String("workspace-mode", envOr("AGED_WORKSPACE_MODE", "isolated"), "worker workspace mode: isolated or shared")
		workspaceRoot    = flag.String("workspace-root", envOr("AGED_WORKSPACE_ROOT", ".aged/workspaces"), "directory for isolated worker workspaces")
		workspaceCleanup = flag.String("workspace-cleanup", envOr("AGED_WORKSPACE_CLEANUP", "retain"), "workspace cleanup policy: retain, delete_on_success, or delete_on_terminal")
		webDistPath      = flag.String("web", envOr("AGED_WEB_DIST", "web/dist"), "built web dashboard directory")
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

	absWorkDir, err := filepath.Abs(*workDir)
	if err != nil {
		slog.Error("resolve workdir", "error", err)
		os.Exit(1)
	}

	var fallbackBrain orchestrator.BrainProvider
	fallbackBrain, err = orchestrator.NewPromptBrain(*workerKind, *promptPath)
	if err != nil {
		slog.Warn("using static brain because prompt template could not be loaded", "error", err)
		fallbackBrain = &orchestrator.StaticBrain{WorkerKind: *workerKind}
	}

	var brain orchestrator.BrainProvider = fallbackBrain
	switch *brainMode {
	case "prompt":
	case "static":
		brain = &orchestrator.StaticBrain{WorkerKind: *workerKind}
	case "codex":
		codexBrain, err := orchestrator.NewCodexBrain(orchestrator.CodexBrainConfig{
			CodexPath:    *codexPath,
			TemplatePath: *schedulerPrompt,
			WorkDir:      absWorkDir,
			Fallback:     fallbackBrain,
		})
		if err != nil {
			slog.Warn("using fallback brain because Codex brain could not be configured", "error", err)
		} else {
			brain = codexBrain
		}
	case "api":
		apiBrain, err := orchestrator.NewAPIBrain(orchestrator.APIBrainConfig{
			Endpoint:     *brainEndpoint,
			APIKey:       *brainAPIKey,
			Model:        *brainModel,
			TemplatePath: *schedulerPrompt,
			Fallback:     fallbackBrain,
		})
		if err != nil {
			slog.Warn("using fallback brain because API brain could not be configured", "error", err)
		} else {
			brain = apiBrain
		}
	default:
		slog.Warn("unknown brain mode; using fallback prompt brain", "brain", *brainMode)
	}

	service := orchestrator.NewServiceWithWorkspaceManager(
		store,
		brain,
		worker.DefaultRunners(),
		absWorkDir,
		orchestrator.NewWorkspaceManager(orchestrator.WorkspaceVCS(*workspaceVCS), orchestrator.WorkspaceMode(*workspaceMode), *workspaceRoot, orchestrator.WorkspaceCleanupPolicy(*workspaceCleanup)),
	)
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

func envFirst(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

func staticHandler(path string) http.Handler {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return http.FileServer(http.Dir(path))
}
