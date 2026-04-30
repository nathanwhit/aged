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
	"strings"
	"syscall"
	"time"

	"aged/internal/eventstore"
	"aged/internal/httpapi"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func main() {
	var (
		addr              = flag.String("addr", envOr("AGED_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		dbPath            = flag.String("db", envOr("AGED_DB", "aged.db"), "SQLite database path")
		workDir           = flag.String("workdir", envOr("AGED_WORKDIR", "."), "worker working directory")
		projectsPath      = flag.String("projects", envOr("AGED_PROJECTS", ""), "JSON project registry config")
		pluginsPath       = flag.String("plugins", envOr("AGED_PLUGINS", ""), "JSON plugin manifest config")
		workerKind        = flag.String("worker", envOr("AGED_DEFAULT_WORKER", "mock"), "orchestrator fallback worker kind")
		assistantMode     = flag.String("assistant", envOr("AGED_ASSISTANT", ""), "interactive assistant provider: auto, brain, none, codex, or claude")
		brainMode         = flag.String("brain", envOr("AGED_BRAIN", "prompt"), "brain provider: prompt, codex, api, or static")
		promptPath        = flag.String("prompt", envOr("AGED_ORCHESTRATOR_PROMPT", "prompts/orchestrator.md"), "fallback worker prompt template")
		schedulerPrompt   = flag.String("scheduler-prompt", envOr("AGED_SCHEDULER_PROMPT", "prompts/scheduler.md"), "API scheduler prompt template")
		brainEndpoint     = flag.String("brain-endpoint", envOr("AGED_BRAIN_ENDPOINT", "https://api.openai.com/v1/chat/completions"), "OpenAI-compatible chat completions endpoint")
		brainAPIKey       = flag.String("brain-api-key", envFirst("AGED_BRAIN_API_KEY", "OPENAI_API_KEY"), "API key for the API brain provider")
		brainModel        = flag.String("brain-model", envOr("AGED_BRAIN_MODEL", ""), "model for the API brain provider")
		codexPath         = flag.String("codex-path", envOr("AGED_CODEX_PATH", "codex"), "Codex CLI path for the codex brain")
		claudePath        = flag.String("claude-path", envOr("AGED_CLAUDE_PATH", "claude"), "Claude CLI path for the assistant")
		workspaceVCS      = flag.String("workspace-vcs", envOr("AGED_WORKSPACE_VCS", "auto"), "worker workspace VCS: auto, jj, or git")
		workspaceMode     = flag.String("workspace-mode", envOr("AGED_WORKSPACE_MODE", "isolated"), "worker workspace mode: isolated or shared")
		workspaceRoot     = flag.String("workspace-root", envOr("AGED_WORKSPACE_ROOT", ".aged/workspaces"), "directory for isolated worker workspaces")
		workspaceCleanup  = flag.String("workspace-cleanup", envOr("AGED_WORKSPACE_CLEANUP", "retain"), "workspace cleanup policy: retain, delete_on_success, or delete_on_terminal")
		targetsPath       = flag.String("targets", envOr("AGED_TARGETS", ""), "JSON execution target pool config")
		githubDriverPath  = flag.String("github-driver", envOr("AGED_GITHUB_DRIVER", ""), "GitHub driver config JSON path or inline JSON")
		discordDriverPath = flag.String("discord-driver", envOr("AGED_DISCORD_DRIVER", ""), "Discord driver config JSON path or inline JSON")
		webDistPath       = flag.String("web", envOr("AGED_WEB_DIST", "web/dist"), "built web dashboard directory")
		authMode          = flag.String("auth", envOr("AGED_AUTH", "none"), "HTTP authentication mode: none or google")
		googleClientID    = flag.String("google-client-id", envOr("AGED_GOOGLE_CLIENT_ID", ""), "Google OAuth client ID")
		googleSecret      = flag.String("google-client-secret", envOr("AGED_GOOGLE_CLIENT_SECRET", ""), "Google OAuth client secret")
		authEmails        = flag.String("auth-allowed-emails", envOr("AGED_AUTH_ALLOWED_EMAILS", ""), "comma-separated Google account emails allowed to access aged")
		authSessionKey    = flag.String("auth-session-key", envOr("AGED_AUTH_SESSION_KEY", ""), "session signing key; use at least 32 random bytes")
		authRedirectURL   = flag.String("auth-redirect-url", envOr("AGED_AUTH_REDIRECT_URL", ""), "public OAuth callback URL, for example https://aged.example.com/auth/callback")
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

	targets, err := orchestrator.LoadTargetRegistry(*targetsPath)
	if err != nil {
		slog.Error("load execution targets", "error", err)
		os.Exit(1)
	}
	projects, err := orchestrator.LoadProjectRegistry(*projectsPath, absWorkDir)
	if err != nil {
		slog.Error("load projects", "error", err)
		os.Exit(1)
	}
	plugins, err := orchestrator.LoadPluginRegistry(*pluginsPath)
	if err != nil {
		slog.Error("load plugins", "error", err)
		os.Exit(1)
	}
	plugins.Probe(ctx)
	plugins.StartDrivers(ctx)

	runners := worker.DefaultRunners()
	for kind, runner := range plugins.RunnerPlugins() {
		runners[kind] = runner
	}
	service := orchestrator.NewServiceWithWorkspaceManagerAndTargets(
		store,
		brain,
		runners,
		absWorkDir,
		orchestrator.NewWorkspaceManager(orchestrator.WorkspaceVCS(*workspaceVCS), orchestrator.WorkspaceMode(*workspaceMode), *workspaceRoot, orchestrator.WorkspaceCleanupPolicy(*workspaceCleanup)),
		targets,
		orchestrator.NewSSHRunner(),
	)
	if err := service.LoadProjects(ctx, projects); err != nil {
		slog.Error("initialize projects", "error", err)
		os.Exit(1)
	}
	service.SetPlugins(plugins)
	assistant, err := configureAssistant(*assistantMode, *workerKind, *brainMode, orchestrator.CLIAssistantConfig{
		CodexPath:  *codexPath,
		ClaudePath: *claudePath,
		WorkDir:    absWorkDir,
	})
	if err != nil {
		slog.Error("configure assistant", "error", err)
		os.Exit(1)
	}
	if assistant != nil {
		service.SetAssistant(assistant)
	}
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		slog.Warn("recover workers", "error", err)
	}
	service.StartTargetProbes(ctx, 30*time.Second)
	githubDriverConfig, err := orchestrator.LoadGitHubDriverConfig(*githubDriverPath)
	if err != nil {
		slog.Error("load github driver", "error", err)
		os.Exit(1)
	}
	if githubDriverConfig.Enabled {
		driver := orchestrator.NewGitHubDriver(service, githubDriverConfig, nil)
		go driver.Run(ctx)
		slog.Info("github driver enabled", "intervalSeconds", githubDriverConfig.IntervalSeconds)
	}
	discordDriverConfig, err := orchestrator.LoadDiscordDriverConfig(*discordDriverPath)
	if err != nil {
		slog.Error("load discord driver", "error", err)
		os.Exit(1)
	}
	if discordDriverConfig.Enabled {
		driver := orchestrator.NewDiscordDriver(service, discordDriverConfig, nil)
		go driver.Run(ctx)
		slog.Info("discord driver enabled", "intervalSeconds", discordDriverConfig.IntervalSeconds, "channels", len(discordDriverConfig.Channels))
	}
	auth, err := configureAuth(*authMode, httpapi.GoogleAuthConfig{
		ClientID:     *googleClientID,
		ClientSecret: *googleSecret,
		AllowedEmail: splitCSV(*authEmails),
		SessionKey:   *authSessionKey,
		RedirectURL:  *authRedirectURL,
	})
	if err != nil {
		slog.Error("configure auth", "error", err)
		os.Exit(1)
	}
	if *authMode == "google" && *authSessionKey == "" {
		slog.Warn("using ephemeral auth session key; sessions will be invalid after daemon restart")
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.NewWithAuth(service, staticHandler(*webDistPath), auth).Routes(),
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

func configureAssistant(mode string, workerKind string, brainMode string, config orchestrator.CLIAssistantConfig) (orchestrator.AssistantProvider, error) {
	mode = strings.TrimSpace(mode)
	if mode == "" || mode == "auto" {
		switch workerKind {
		case "codex", "claude":
			mode = workerKind
		case "codex-cli":
			mode = "codex"
		default:
			if brainMode == "codex" || brainMode == "api" {
				return nil, nil
			}
			mode = "none"
		}
	}
	if mode == "brain" || mode == "none" {
		return nil, nil
	}
	config.Kind = mode
	return orchestrator.NewCLIAssistant(config)
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

func configureAuth(mode string, config httpapi.GoogleAuthConfig) (*httpapi.GoogleAuth, error) {
	switch mode {
	case "", "none":
		return nil, nil
	case "google":
		return httpapi.NewGoogleAuth(config)
	default:
		return nil, errors.New("unknown auth mode")
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func staticHandler(path string) http.Handler {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return http.FileServer(http.Dir(path))
}
