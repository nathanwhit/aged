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

	"aged/internal/envutil"
	"aged/internal/eventstore"
	"aged/internal/flagutil"
	"aged/internal/httpapi"
	"aged/internal/orchestrator"
	"aged/internal/worker"
)

func main() {
	var (
		addr              = flag.String("addr", envutil.String("AGED_ADDR", "127.0.0.1:8787"), "HTTP listen address")
		dbPath            = flag.String("db", envutil.String("AGED_DB", "aged.db"), "SQLite database path")
		workDir           = flag.String("workdir", envutil.String("AGED_WORKDIR", "."), "worker working directory")
		projectsPath      = flag.String("projects", envutil.String("AGED_PROJECTS", ""), "JSON project registry config")
		pluginsPath       = flag.String("plugins", envutil.String("AGED_PLUGINS", ""), "JSON plugin manifest config")
		workerKind        = flag.String("worker", envutil.String("AGED_DEFAULT_WORKER", "mock"), "orchestrator fallback worker kind")
		assistantMode     = flag.String("assistant", envutil.String("AGED_ASSISTANT", ""), "interactive assistant provider: auto, brain, none, codex, or claude")
		assistantReason   = flag.String("assistant-reasoning", envutil.String("AGED_ASSISTANT_REASONING", "medium"), "interactive assistant reasoning effort: default, low, medium, high, xhigh, or max")
		brainMode         = flag.String("brain", envutil.String("AGED_BRAIN", "prompt"), "brain provider: prompt, codex, claude, api, or static")
		promptPath        = flag.String("prompt", envutil.String("AGED_ORCHESTRATOR_PROMPT", "prompts/orchestrator.md"), "fallback worker prompt template")
		schedulerPrompt   = flag.String("scheduler-prompt", envutil.String("AGED_SCHEDULER_PROMPT", "prompts/default/system.md"), "API scheduler prompt template")
		promptDir         = flag.String("prompt-dir", envutil.String("AGED_PROMPT_DIR", "prompts/default"), "directory containing built-in scheduler prompt set templates")
		promptSetID       = flag.String("prompt-set", envutil.String("AGED_PROMPT_SET", ""), "default scheduler prompt set id")
		brainEndpoint     = flag.String("brain-endpoint", envutil.String("AGED_BRAIN_ENDPOINT", "https://api.openai.com/v1/chat/completions"), "OpenAI-compatible chat completions endpoint")
		brainAPIKey       = flag.String("brain-api-key", envutil.First("AGED_BRAIN_API_KEY", "OPENAI_API_KEY"), "API key for the API brain provider")
		brainModel        = flag.String("brain-model", envutil.String("AGED_BRAIN_MODEL", ""), "model for the API brain provider")
		codexPath         = flag.String("codex-path", envutil.String("AGED_CODEX_PATH", "codex"), "Codex CLI path for Codex-backed brain, assistant, and worker defaults")
		claudePath        = flag.String("claude-path", envutil.String("AGED_CLAUDE_PATH", "claude"), "Claude CLI path for Claude-backed brain, assistant, and worker defaults")
		workspaceVCS      = flag.String("workspace-vcs", envutil.String("AGED_WORKSPACE_VCS", "auto"), "worker workspace VCS: auto, jj, or git")
		workspaceMode     = flag.String("workspace-mode", envutil.String("AGED_WORKSPACE_MODE", "isolated"), "worker workspace mode: isolated or shared")
		workspaceRoot     = flag.String("workspace-root", envutil.String("AGED_WORKSPACE_ROOT", ""), "directory for isolated worker workspaces; empty defaults to ~/.aged/workspaces")
		workspaceCleanup  = flag.String("workspace-cleanup", envutil.String("AGED_WORKSPACE_CLEANUP", "retain"), "workspace cleanup policy: retain, delete_on_success, or delete_on_terminal")
		artifactCleanup   = flag.Bool("workspace-artifact-cleanup", envutil.Bool("AGED_WORKSPACE_ARTIFACT_CLEANUP", true), "remove allowlisted build artifact directories from stale retained worker workspaces")
		artifactDryRun    = flag.Bool("workspace-artifact-cleanup-dry-run", envutil.Bool("AGED_WORKSPACE_ARTIFACT_CLEANUP_DRY_RUN", false), "report stale retained worker artifact cleanup without deleting directories")
		artifactMinAge    = flag.Duration("workspace-artifact-cleanup-min-age", envutil.Duration("AGED_WORKSPACE_ARTIFACT_CLEANUP_MIN_AGE", 24*time.Hour), "minimum terminal worker age before retained workspace artifact cleanup")
		artifactInterval  = flag.Duration("workspace-artifact-cleanup-interval", envutil.Duration("AGED_WORKSPACE_ARTIFACT_CLEANUP_INTERVAL", time.Hour), "interval between retained worker artifact cleanup scans")
		targetsPath       = flag.String("targets", envutil.String("AGED_TARGETS", ""), "JSON execution target pool config")
		githubDriverPath  = flagutil.NewOptionalValue(envutil.String("AGED_GITHUB_DRIVER", ""))
		prMonitor         = flag.Bool("pull-request-monitor", envutil.Bool("AGED_PULL_REQUEST_MONITOR", true), "periodically refresh tracked pull requests and resume tasks that need follow-up")
		prMonitorInterval = flag.Duration("pull-request-monitor-interval", envutil.Duration("AGED_PULL_REQUEST_MONITOR_INTERVAL", time.Minute), "tracked pull request refresh interval")
		usageAware        = flag.Bool("usage-aware-scheduling", envutil.Bool("AGED_USAGE_AWARE_SCHEDULING", true), "rebalance Codex and Claude worker scheduling using cached interactive CLI usage probes")
		usageTTL          = flag.Duration("usage-aware-scheduling-ttl", envutil.Duration("AGED_USAGE_AWARE_SCHEDULING_TTL", 5*time.Minute), "minimum interval between Codex/Claude usage probes")
		discordDriverPath = flag.String("discord-driver", envutil.String("AGED_DISCORD_DRIVER", ""), "Discord driver config JSON path or inline JSON")
		webDistPath       = flag.String("web", envutil.String("AGED_WEB_DIST", "web/dist"), "built web dashboard directory")
		authMode          = flag.String("auth", envutil.String("AGED_AUTH", "none"), "HTTP authentication mode: none or google")
		googleClientID    = flag.String("google-client-id", envutil.String("AGED_GOOGLE_CLIENT_ID", ""), "Google OAuth client ID")
		googleSecret      = flag.String("google-client-secret", envutil.String("AGED_GOOGLE_CLIENT_SECRET", ""), "Google OAuth client secret")
		authEmails        = flag.String("auth-allowed-emails", envutil.String("AGED_AUTH_ALLOWED_EMAILS", ""), "comma-separated Google account emails allowed to access aged")
		authSessionKey    = flag.String("auth-session-key", envutil.String("AGED_AUTH_SESSION_KEY", ""), "session signing key; use at least 32 random bytes")
		authRedirectURL   = flag.String("auth-redirect-url", envutil.String("AGED_AUTH_REDIRECT_URL", ""), "public OAuth callback URL, for example https://aged.example.com/auth/callback")
	)
	flag.Var(githubDriverPath, "github-driver", "enable GitHub driver; optionally accepts config JSON path or inline JSON")
	flag.CommandLine.Parse(flagutil.NormalizeOptionalValueArgs(os.Args[1:], "github-driver"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := eventstore.OpenSQLite(ctx, *dbPath)
	if err != nil {
		fatal("open event store", err)
	}
	defer store.Close()

	absWorkDir, err := filepath.Abs(*workDir)
	if err != nil {
		fatal("resolve workdir", err)
	}

	var fallbackBrain orchestrator.BrainProvider
	fallbackBrain, err = orchestrator.NewPromptBrain(*workerKind, *promptPath)
	if err != nil {
		slog.Warn("using static brain because prompt template could not be loaded", "error", err)
		fallbackBrain = &orchestrator.StaticBrain{WorkerKind: *workerKind}
	}

	storedPromptSets, storedPromptSetID, err := store.ListPromptSets(ctx)
	if err != nil {
		fatal("load prompt sets", err)
	}
	defaultPromptSet, err := orchestrator.LoadDefaultPromptSet(*promptDir)
	if err != nil {
		slog.Warn("using hardcoded prompt fallback because default prompt set could not be loaded", "path", *promptDir, "error", err)
	} else {
		storedPromptSets = append(storedPromptSets, defaultPromptSet)
	}
	defaultPromptSetID := strings.TrimSpace(*promptSetID)
	if defaultPromptSetID == "" {
		defaultPromptSetID = storedPromptSetID
	}
	promptSets := orchestrator.NewPromptSetRegistry(storedPromptSets, defaultPromptSetID)
	var brain orchestrator.BrainProvider = fallbackBrain
	switch *brainMode {
	case "prompt":
	case "static":
		brain = &orchestrator.StaticBrain{WorkerKind: *workerKind}
	case "codex":
		codexBrain, err := orchestrator.NewCodexBrain(orchestrator.CodexBrainConfig{
			CodexPath:    *codexPath,
			TemplatePath: *schedulerPrompt,
			PromptSets:   promptSets,
			WorkDir:      absWorkDir,
			Fallback:     fallbackBrain,
		})
		if err != nil {
			slog.Warn("using fallback brain because Codex brain could not be configured", "error", err)
		} else {
			brain = codexBrain
		}
	case "claude":
		claudeBrain, err := orchestrator.NewClaudeBrain(orchestrator.ClaudeBrainConfig{
			ClaudePath:   *claudePath,
			TemplatePath: *schedulerPrompt,
			PromptSets:   promptSets,
			WorkDir:      absWorkDir,
			Fallback:     fallbackBrain,
		})
		if err != nil {
			slog.Warn("using fallback brain because Claude brain could not be configured", "error", err)
		} else {
			brain = claudeBrain
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
		fatal("load execution targets", err)
	}
	projects, err := orchestrator.LoadProjectRegistry(*projectsPath, absWorkDir)
	if err != nil {
		fatal("load projects", err)
	}
	plugins, err := orchestrator.LoadPluginRegistry(*pluginsPath)
	if err != nil {
		fatal("load plugins", err)
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
	if err := service.LoadRegisteredTargets(ctx); err != nil {
		fatal("initialize registered targets", err)
	}
	service.SetPromptSets(promptSets)
	if *usageAware {
		service.SetProviderUsageSource(orchestrator.NewTmuxProviderUsageMonitor("tmux", *codexPath, *claudePath, *usageTTL))
	}
	if err := service.LoadProjects(ctx, projects); err != nil {
		fatal("initialize projects", err)
	}
	service.SetPluginRuntimeContext(ctx)
	service.SetPlugins(plugins)
	if err := service.LoadRegisteredPlugins(ctx); err != nil {
		fatal("initialize registered plugins", err)
	}
	assistant, err := configureAssistant(*assistantMode, *workerKind, *brainMode, orchestrator.CLIAssistantConfig{
		CodexPath:       *codexPath,
		ClaudePath:      *claudePath,
		WorkDir:         absWorkDir,
		ReasoningEffort: *assistantReason,
	})
	if err != nil {
		fatal("configure assistant", err)
	}
	if assistant != nil {
		service.SetAssistant(assistant)
	}
	if err := service.RecoverRemoteWorkers(ctx); err != nil {
		slog.Warn("recover workers", "error", err)
	}
	if *artifactCleanup {
		cleanupOptions := orchestrator.RetainedWorkspaceArtifactCleanupOptions{
			MinAge: *artifactMinAge,
			DryRun: *artifactDryRun,
		}
		report, err := service.CleanupRetainedWorkspaceArtifacts(ctx, cleanupOptions)
		if err != nil {
			slog.Warn("cleanup retained workspace artifacts", "error", err)
		} else if report.Scanned > 0 {
			slog.Info("cleanup retained workspace artifacts", "scanned", report.Scanned, "cleaned", report.Cleaned, "skipped", report.Skipped, "bytesRemoved", report.BytesRemoved, "dryRun", report.DryRun)
		}
		service.StartRetainedWorkspaceArtifactCleanup(ctx, *artifactInterval, cleanupOptions)
	}
	service.StartTargetProbes(ctx, 30*time.Second)
	if *prMonitor {
		service.StartPullRequestMonitor(ctx, *prMonitorInterval)
		slog.Info("pull request monitor enabled", "interval", prMonitorInterval.String())
	}
	githubDriverConfig, err := orchestrator.LoadGitHubDriverConfig(githubDriverPath.String())
	if err != nil {
		fatal("load github driver", err)
	}
	githubDriverState, err := service.Drivers().StartGitHubDriver(ctx, githubDriverConfig)
	if err != nil {
		fatal("start github driver", err)
	}
	if githubDriverState.Config.Enabled {
		slog.Info("github driver enabled", "intervalSeconds", githubDriverConfig.IntervalSeconds)
	}
	discordDriverConfig, err := orchestrator.LoadDiscordDriverConfig(*discordDriverPath)
	if err != nil {
		fatal("load discord driver", err)
	}
	discordDriverState, err := service.Drivers().StartDiscordDriver(ctx, discordDriverConfig)
	if err != nil {
		fatal("start discord driver", err)
	}
	if discordDriverState.Config.Enabled {
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
		fatal("configure auth", err)
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
			fatal("serve", err)
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
			if brainMode == "codex" || brainMode == "claude" || brainMode == "api" {
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

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
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
