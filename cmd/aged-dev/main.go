package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"aged/internal/envutil"
	"aged/internal/flagutil"
)

type devServer struct {
	mu sync.Mutex

	repoRoot   string
	binaryPath string
	logPath    string
	daemonAddr string
	daemonArgs []string
	daemonEnv  []string

	cmd       *exec.Cmd
	lastRun   runResult
	rebuildID int
}

type runResult struct {
	ID        int       `json:"id"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
	OK        bool      `json:"ok"`
	Running   bool      `json:"running"`
	Output    string    `json:"output"`
	Error     string    `json:"error,omitempty"`
	PID       int       `json:"pid,omitempty"`
}

type daemonConfig struct {
	daemonAddr        string
	dbPath            string
	workDir           string
	projectsPath      string
	pluginsPath       string
	workerKind        string
	assistantMode     string
	assistantReason   string
	brainMode         string
	workspaceVCS      string
	workspaceMode     string
	workspaceRoot     string
	workspaceCleanup  string
	artifactCleanup   bool
	artifactDryRun    bool
	artifactMinAge    time.Duration
	artifactInterval  time.Duration
	usageAware        bool
	usageTTL          time.Duration
	githubDriverPath  string
	discordDriverPath string
	webDistPath       string
}

func main() {
	var (
		addr              = flag.String("addr", envutil.String("AGED_DEV_ADDR", "127.0.0.1:8790"), "dev control server listen address")
		daemonAddr        = flag.String("daemon-addr", envutil.String("AGED_ADDR", "127.0.0.1:8787"), "aged daemon listen address")
		dbPath            = flag.String("db", envutil.String("AGED_DB", "aged.db"), "aged daemon SQLite database path")
		workDir           = flag.String("workdir", envutil.String("AGED_WORKDIR", "."), "aged daemon worker directory")
		projectsPath      = flag.String("projects", envutil.String("AGED_PROJECTS", ""), "aged daemon project registry config")
		pluginsPath       = flag.String("plugins", envutil.String("AGED_PLUGINS", ""), "aged daemon plugin manifest config")
		workerKind        = flag.String("worker", envutil.String("AGED_DEFAULT_WORKER", "codex"), "aged daemon fallback worker kind")
		assistantMode     = flag.String("assistant", envutil.String("AGED_ASSISTANT", "auto"), "aged daemon assistant provider")
		assistantReason   = flag.String("assistant-reasoning", envutil.String("AGED_ASSISTANT_REASONING", "medium"), "aged daemon assistant reasoning effort")
		brainMode         = flag.String("brain", envutil.String("AGED_BRAIN", "prompt"), "aged daemon brain provider")
		workspaceVCS      = flag.String("workspace-vcs", envutil.String("AGED_WORKSPACE_VCS", "auto"), "aged daemon workspace VCS")
		workspaceMode     = flag.String("workspace-mode", envutil.String("AGED_WORKSPACE_MODE", "isolated"), "aged daemon workspace mode")
		workspaceRoot     = flag.String("workspace-root", envutil.String("AGED_WORKSPACE_ROOT", ""), "aged daemon workspace root; empty defaults to ~/.aged/workspaces")
		workspaceCleanup  = flag.String("workspace-cleanup", envutil.String("AGED_WORKSPACE_CLEANUP", "retain"), "aged daemon workspace cleanup policy")
		artifactCleanup   = flag.Bool("workspace-artifact-cleanup", envutil.Bool("AGED_WORKSPACE_ARTIFACT_CLEANUP", true), "aged daemon retained workspace artifact cleanup")
		artifactDryRun    = flag.Bool("workspace-artifact-cleanup-dry-run", envutil.Bool("AGED_WORKSPACE_ARTIFACT_CLEANUP_DRY_RUN", false), "aged daemon retained workspace artifact cleanup dry run")
		artifactMinAge    = flag.Duration("workspace-artifact-cleanup-min-age", envutil.Duration("AGED_WORKSPACE_ARTIFACT_CLEANUP_MIN_AGE", 24*time.Hour), "aged daemon retained workspace artifact cleanup minimum age")
		artifactInterval  = flag.Duration("workspace-artifact-cleanup-interval", envutil.Duration("AGED_WORKSPACE_ARTIFACT_CLEANUP_INTERVAL", time.Hour), "aged daemon retained workspace artifact cleanup scan interval")
		usageAware        = flag.Bool("usage-aware-scheduling", envutil.Bool("AGED_USAGE_AWARE_SCHEDULING", true), "aged daemon Codex/Claude usage-aware worker scheduling")
		usageTTL          = flag.Duration("usage-aware-scheduling-ttl", envutil.Duration("AGED_USAGE_AWARE_SCHEDULING_TTL", 5*time.Minute), "aged daemon Codex/Claude usage probe cache ttl")
		githubDriverPath  = flagutil.NewOptionalValue(envutil.String("AGED_GITHUB_DRIVER", ""))
		discordDriverPath = flag.String("discord-driver", envutil.String("AGED_DISCORD_DRIVER", ""), "aged daemon Discord driver config JSON path or inline JSON")
		webDistPath       = flag.String("web", envutil.String("AGED_WEB_DIST", "web/dist"), "aged dashboard dist directory")
		start             = flag.Bool("start", true, "build and start aged immediately")
	)
	flag.Var(githubDriverPath, "github-driver", "enable aged daemon GitHub driver; optionally accepts config JSON path or inline JSON")
	flag.CommandLine.Parse(flagutil.NormalizeOptionalValueArgs(os.Args[1:], "github-driver"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repoRoot, err := os.Getwd()
	if err != nil {
		slog.Error("read working directory", "error", err)
		os.Exit(1)
	}
	binaryPath := filepath.Join(repoRoot, ".aged", "dev", "aged")
	logPath := filepath.Join(repoRoot, ".aged", "dev", "aged.log")
	control := &devServer{
		repoRoot:   repoRoot,
		binaryPath: binaryPath,
		logPath:    logPath,
		daemonAddr: *daemonAddr,
		daemonArgs: buildDaemonArgs(daemonConfig{
			daemonAddr:        *daemonAddr,
			dbPath:            *dbPath,
			workerKind:        *workerKind,
			assistantMode:     *assistantMode,
			assistantReason:   *assistantReason,
			brainMode:         *brainMode,
			workDir:           *workDir,
			projectsPath:      *projectsPath,
			pluginsPath:       *pluginsPath,
			workspaceVCS:      *workspaceVCS,
			workspaceMode:     *workspaceMode,
			workspaceRoot:     *workspaceRoot,
			workspaceCleanup:  *workspaceCleanup,
			artifactCleanup:   *artifactCleanup,
			artifactDryRun:    *artifactDryRun,
			artifactMinAge:    *artifactMinAge,
			artifactInterval:  *artifactInterval,
			usageAware:        *usageAware,
			usageTTL:          *usageTTL,
			githubDriverPath:  githubDriverPath.String(),
			discordDriverPath: *discordDriverPath,
			webDistPath:       *webDistPath,
		}),
		daemonEnv: os.Environ(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /status", control.status)
	mux.HandleFunc("GET /rebuild", control.rebuild)
	mux.HandleFunc("POST /rebuild", control.rebuild)

	if *start {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if result := control.rebuildAndRestart(ctx); !result.OK {
				slog.Error("initial rebuild failed", "error", result.Error)
			}
		}()
	}

	slog.Info("aged dev control server listening", "addr", "http://"+*addr, "daemon", "http://"+*daemonAddr)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("serve", "error", err)
	}
	var log bytes.Buffer
	control.mu.Lock()
	if err := control.stopDaemon(&log); err != nil {
		slog.Error("stop managed daemon", "error", err)
	}
	control.mu.Unlock()
}

func buildDaemonArgs(config daemonConfig) []string {
	return []string{
		"-addr", config.daemonAddr,
		"-db", config.dbPath,
		"-worker", config.workerKind,
		"-assistant", config.assistantMode,
		"-assistant-reasoning", config.assistantReason,
		"-brain", config.brainMode,
		"-workdir", config.workDir,
		"-projects", config.projectsPath,
		"-plugins", config.pluginsPath,
		"-workspace-vcs", config.workspaceVCS,
		"-workspace-mode", config.workspaceMode,
		"-workspace-root", config.workspaceRoot,
		"-workspace-cleanup", config.workspaceCleanup,
		"-workspace-artifact-cleanup=" + strconv.FormatBool(config.artifactCleanup),
		"-workspace-artifact-cleanup-dry-run=" + strconv.FormatBool(config.artifactDryRun),
		"-workspace-artifact-cleanup-min-age", config.artifactMinAge.String(),
		"-workspace-artifact-cleanup-interval", config.artifactInterval.String(),
		"-usage-aware-scheduling=" + strconv.FormatBool(config.usageAware),
		"-usage-aware-scheduling-ttl", config.usageTTL.String(),
		"-github-driver=" + config.githubDriverPath,
		"-discord-driver", config.discordDriverPath,
		"-web", config.webDistPath,
	}
}

func (s *devServer) status(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.lastRun
	result.Running = s.cmd != nil && s.cmd.Process != nil
	if result.Running {
		result.PID = s.cmd.Process.Pid
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *devServer) rebuild(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	result := s.rebuildAndRestart(ctx)
	status := http.StatusOK
	if !result.OK {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, result)
}

func (s *devServer) rebuildAndRestart(ctx context.Context) runResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rebuildID++
	result := runResult{
		ID:        s.rebuildID,
		StartedAt: time.Now().UTC(),
	}
	var log bytes.Buffer
	finish := func(err error) runResult {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		if err != nil {
			result.Error = err.Error()
		}
		s.lastRun = result
		return result
	}

	if err := s.stopDaemon(&log); err != nil {
		return finish(err)
	}
	if err := s.killDaemonPortListeners(ctx, &log); err != nil {
		return finish(err)
	}
	if err := os.MkdirAll(filepath.Dir(s.binaryPath), 0o755); err != nil {
		return finish(err)
	}
	if err := runCommand(ctx, s.repoRoot, &log, "go", "build", "-o", s.binaryPath, "./cmd/aged"); err != nil {
		return finish(err)
	}
	if err := runCommand(ctx, filepath.Join(s.repoRoot, "web"), &log, "npm", "run", "build"); err != nil {
		return finish(err)
	}
	cmd, err := s.startDaemon(&log)
	if err != nil {
		return finish(err)
	}
	result.OK = true
	result.Running = true
	result.PID = cmd.Process.Pid
	return finish(nil)
}

func (s *devServer) stopDaemon(log *bytes.Buffer) error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	fmt.Fprintf(log, "$ stop aged pid %d\n", s.cmd.Process.Pid)
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	case <-done:
	}
	s.cmd = nil
	return nil
}

func (s *devServer) killDaemonPortListeners(ctx context.Context, log *bytes.Buffer) error {
	port, err := portFromAddr(s.daemonAddr)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "lsof", lsofListenArgs(port)...)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil
		}
		return fmt.Errorf("find daemon port listeners: %w", err)
	}
	for _, field := range strings.Fields(string(out)) {
		pid, err := strconv.Atoi(field)
		if err != nil || pid == os.Getpid() {
			continue
		}
		fmt.Fprintf(log, "$ kill existing daemon listener pid %d\n", pid)
		process, err := os.FindProcess(pid)
		if err != nil {
			return err
		}
		if err := process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
	}
	return nil
}

func (s *devServer) startDaemon(log *bytes.Buffer) (*exec.Cmd, error) {
	fmt.Fprintf(log, "$ %s %s\n", s.binaryPath, strings.Join(s.daemonArgs, " "))
	daemonLog, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(log, "$ daemon log: %s\n", s.logPath)
	cmd := exec.Command(s.binaryPath, s.daemonArgs...)
	cmd.Dir = s.repoRoot
	cmd.Env = s.daemonEnv
	cmd.Stdout = daemonLog
	cmd.Stderr = daemonLog
	if err := cmd.Start(); err != nil {
		_ = daemonLog.Close()
		return nil, err
	}
	s.cmd = cmd
	go func() {
		_ = cmd.Wait()
		_ = daemonLog.Close()
	}()
	return cmd, nil
}

func runCommand(ctx context.Context, dir string, log *bytes.Buffer, name string, args ...string) error {
	fmt.Fprintf(log, "$ %s %s\n", name, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

func lsofListenArgs(port string) []string {
	return []string{"-ti", "-sTCP:LISTEN", "-iTCP:" + port}
}

func portFromAddr(addr string) (string, error) {
	_, port, err := net.SplitHostPort(addr)
	if err == nil {
		return port, nil
	}
	if strings.Count(addr, ":") == 0 {
		return addr, nil
	}
	return "", fmt.Errorf("parse daemon addr %q: %w", addr, err)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
