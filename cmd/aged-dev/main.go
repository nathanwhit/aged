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

func main() {
	var (
		addr             = flag.String("addr", envOr("AGED_DEV_ADDR", "127.0.0.1:8790"), "dev control server listen address")
		daemonAddr       = flag.String("daemon-addr", envOr("AGED_ADDR", "127.0.0.1:8787"), "aged daemon listen address")
		dbPath           = flag.String("db", envOr("AGED_DB", "aged.db"), "aged daemon SQLite database path")
		workDir          = flag.String("workdir", envOr("AGED_WORKDIR", "."), "aged daemon worker directory")
		workerKind       = flag.String("worker", envOr("AGED_DEFAULT_WORKER", "codex"), "aged daemon fallback worker kind")
		brainMode        = flag.String("brain", envOr("AGED_BRAIN", "prompt"), "aged daemon brain provider")
		workspaceVCS     = flag.String("workspace-vcs", envOr("AGED_WORKSPACE_VCS", "auto"), "aged daemon workspace VCS")
		workspaceMode    = flag.String("workspace-mode", envOr("AGED_WORKSPACE_MODE", "isolated"), "aged daemon workspace mode")
		workspaceRoot    = flag.String("workspace-root", envOr("AGED_WORKSPACE_ROOT", ".aged/workspaces"), "aged daemon workspace root")
		workspaceCleanup = flag.String("workspace-cleanup", envOr("AGED_WORKSPACE_CLEANUP", "retain"), "aged daemon workspace cleanup policy")
		webDistPath      = flag.String("web", envOr("AGED_WEB_DIST", "web/dist"), "aged dashboard dist directory")
		start            = flag.Bool("start", true, "build and start aged immediately")
	)
	flag.Parse()

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
		daemonArgs: []string{
			"-addr", *daemonAddr,
			"-db", *dbPath,
			"-worker", *workerKind,
			"-brain", *brainMode,
			"-workdir", *workDir,
			"-workspace-vcs", *workspaceVCS,
			"-workspace-mode", *workspaceMode,
			"-workspace-root", *workspaceRoot,
			"-workspace-cleanup", *workspaceCleanup,
			"-web", *webDistPath,
		},
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

	if err := s.stopDaemon(&log); err != nil {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	if err := s.killDaemonPortListeners(ctx, &log); err != nil {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	if err := os.MkdirAll(filepath.Dir(s.binaryPath), 0o755); err != nil {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	if err := runCommand(ctx, s.repoRoot, &log, "go", "build", "-o", s.binaryPath, "./cmd/aged"); err != nil {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	if err := runCommand(ctx, filepath.Join(s.repoRoot, "web"), &log, "npm", "run", "build"); err != nil {
		result.EndedAt = time.Now().UTC()
		result.Output = log.String()
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	cmd, err := s.startDaemon(&log)
	result.EndedAt = time.Now().UTC()
	result.Output = log.String()
	if err != nil {
		result.Error = err.Error()
		s.lastRun = result
		return result
	}
	result.OK = true
	result.Running = true
	result.PID = cmd.Process.Pid
	s.lastRun = result
	return result
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
	cmd := exec.CommandContext(ctx, "lsof", "-ti", "-sTCP:LISTEN", "tcp:"+port)
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

func envOr(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
