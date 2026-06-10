package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	defaultProviderUsageTTL     = 5 * time.Minute
	defaultProviderProbeTimeout = 12 * time.Second
	providerUsageSwitchMargin   = 10
)

type ProviderUsageExhaustion struct {
	Provider string
	Summary  string
	Detail   string
}

type ProviderUsageSource interface {
	Snapshot(ctx context.Context) ProviderUsageSnapshot
}

type ProviderUsageSnapshot struct {
	Providers map[string]ProviderUsage `json:"providers,omitempty"`
	CheckedAt time.Time                `json:"checkedAt,omitempty"`
}

type ProviderUsage struct {
	Kind       string                `json:"kind"`
	Available  bool                  `json:"available"`
	Windows    []ProviderUsageWindow `json:"windows,omitempty"`
	CheckedAt  time.Time             `json:"checkedAt,omitempty"`
	Confidence string                `json:"confidence,omitempty"`
	Error      string                `json:"error,omitempty"`
	Raw        string                `json:"-"`
}

type ProviderUsageWindow struct {
	Name        string `json:"name"`
	UsedPercent int    `json:"usedPercent"`
	Reset       string `json:"reset,omitempty"`
}

type TmuxProviderUsageMonitor struct {
	mu         sync.Mutex
	tmuxPath   string
	codexPath  string
	claudePath string
	ttl        time.Duration
	timeout    time.Duration
	now        func() time.Time
	cache      ProviderUsageSnapshot
}

func NewTmuxProviderUsageMonitor(tmuxPath, codexPath, claudePath string, ttl time.Duration) *TmuxProviderUsageMonitor {
	if strings.TrimSpace(tmuxPath) == "" {
		tmuxPath = "tmux"
	}
	if strings.TrimSpace(codexPath) == "" {
		codexPath = "codex"
	}
	if strings.TrimSpace(claudePath) == "" {
		claudePath = "claude"
	}
	if ttl <= 0 {
		ttl = defaultProviderUsageTTL
	}
	return &TmuxProviderUsageMonitor{
		tmuxPath:   tmuxPath,
		codexPath:  codexPath,
		claudePath: claudePath,
		ttl:        ttl,
		timeout:    defaultProviderProbeTimeout,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

func (m *TmuxProviderUsageMonitor) Snapshot(ctx context.Context) ProviderUsageSnapshot {
	if m == nil {
		return ProviderUsageSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if !m.cache.CheckedAt.IsZero() && now.Sub(m.cache.CheckedAt) < m.ttl {
		return m.cache
	}

	snapshot := ProviderUsageSnapshot{
		Providers: map[string]ProviderUsage{},
		CheckedAt: now,
	}
	snapshot.Providers["codex"] = m.probe(ctx, "codex", m.codexPath, "/status", parseCodexStatusUsage)
	snapshot.Providers["claude"] = m.probe(ctx, "claude", m.claudePath, "/usage", parseClaudeUsage)
	m.cache = snapshot
	return snapshot
}

func (m *TmuxProviderUsageMonitor) probe(ctx context.Context, kind string, command string, slashCommand string, parse func(string, time.Time) ProviderUsage) ProviderUsage {
	checkedAt := m.now()
	output, err := captureProviderSlashCommand(ctx, m.tmuxPath, command, slashCommand, m.timeout)
	if err != nil {
		return ProviderUsage{Kind: kind, CheckedAt: checkedAt, Confidence: "none", Error: err.Error()}
	}
	usage := parse(output, checkedAt)
	usage.Kind = kind
	usage.Raw = output
	if len(usage.Windows) == 0 && usage.Error == "" {
		usage.Error = "usage output did not contain parseable limit windows"
		usage.Confidence = "none"
	}
	return usage
}

func captureProviderSlashCommand(ctx context.Context, tmuxPath string, command string, slashCommand string, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = defaultProviderProbeTimeout
	}
	if _, err := exec.LookPath(command); err != nil {
		return "", err
	}
	session := fmt.Sprintf("aged-usage-%d", time.Now().UnixNano())
	if err := runTmux(ctx, tmuxPath, "new-session", "-d", "-s", session); err != nil {
		return "", err
	}
	defer func() {
		_ = runTmux(context.Background(), tmuxPath, "kill-session", "-t", session)
	}()

	if err := sendTmuxLiteral(ctx, tmuxPath, session, command); err != nil {
		return "", err
	}
	if err := runTmux(ctx, tmuxPath, "send-keys", "-t", session, "Enter"); err != nil {
		return "", err
	}
	if err := waitForPaneText(ctx, tmuxPath, session, timeout, func(text string) bool {
		clean := stripANSI(text)
		return strings.Contains(clean, "Claude Code") || strings.Contains(clean, "OpenAI Codex")
	}); err != nil {
		return "", err
	}
	if err := sendTmuxLiteral(ctx, tmuxPath, session, slashCommand); err != nil {
		return "", err
	}
	if err := runTmux(ctx, tmuxPath, "send-keys", "-t", session, "Enter"); err != nil {
		return "", err
	}
	var output string
	err := waitForPaneText(ctx, tmuxPath, session, timeout, func(text string) bool {
		output = text
		clean := stripANSI(text)
		if slashCommand == "/usage" {
			return strings.Contains(clean, "Current session") || strings.Contains(clean, "Current week")
		}
		return strings.Contains(clean, "5h limit:") || strings.Contains(clean, "Weekly limit:") || strings.Contains(clean, "rate limits and credits")
	})
	if err != nil {
		captured, captureErr := capturePane(ctx, tmuxPath, session)
		if captureErr == nil {
			output = captured
		}
		return output, err
	}
	return output, nil
}

func waitForPaneText(ctx context.Context, tmuxPath string, session string, timeout time.Duration, ready func(string) bool) error {
	deadline := time.Now().Add(timeout)
	for {
		text, err := capturePane(ctx, tmuxPath, session)
		if err != nil {
			return err
		}
		if ready(text) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for provider usage output")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func capturePane(ctx context.Context, tmuxPath string, session string) (string, error) {
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, tmuxPath, "capture-pane", "-t", session, "-p", "-e", "-S", "-200")
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out.String(), nil
}

func sendTmuxLiteral(ctx context.Context, tmuxPath string, session string, text string) error {
	return runTmux(ctx, tmuxPath, "send-keys", "-t", session, "-l", text)
}

func runTmux(ctx context.Context, tmuxPath string, args ...string) error {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, tmuxPath, args...)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return fmt.Errorf("%w: %s", err, detail)
		}
		return err
	}
	return nil
}

func parseClaudeUsage(output string, checkedAt time.Time) ProviderUsage {
	clean := normalizeProviderUsageText(output)
	usage := ProviderUsage{Kind: "claude", Available: true, CheckedAt: checkedAt, Confidence: "medium"}
	lines := nonEmptyUsageLines(clean)
	for i, line := range lines {
		name := strings.TrimSpace(line)
		if !strings.HasPrefix(name, "Current ") {
			continue
		}
		for j := i + 1; j < len(lines) && j <= i+4; j++ {
			percent, ok := percentBeforeWord(lines[j], "used")
			if !ok {
				continue
			}
			window := ProviderUsageWindow{Name: name, UsedPercent: percent}
			for k := j + 1; k < len(lines) && k <= j+3; k++ {
				if reset, ok := strings.CutPrefix(lines[k], "Resets "); ok {
					window.Reset = strings.TrimSpace(reset)
					break
				}
			}
			usage.Windows = append(usage.Windows, window)
			break
		}
	}
	if len(usage.Windows) == 0 {
		usage.Available = false
		usage.Confidence = "none"
	}
	return usage
}

func parseCodexStatusUsage(output string, checkedAt time.Time) ProviderUsage {
	clean := normalizeProviderUsageText(output)
	usage := ProviderUsage{Kind: "codex", Available: true, CheckedAt: checkedAt, Confidence: "medium"}
	lines := nonEmptyUsageLines(clean)
	for i, line := range lines {
		name := ""
		switch {
		case strings.Contains(line, "5h limit:"):
			name = "5h limit"
		case strings.Contains(line, "Weekly limit:"):
			name = "Weekly limit"
		default:
			continue
		}
		percent, ok := percentBeforeWord(line, "left")
		if !ok {
			for j := i + 1; j < len(lines) && j <= i+2; j++ {
				if percent, ok = percentBeforeWord(lines[j], "left"); ok {
					break
				}
			}
		}
		if !ok {
			continue
		}
		window := ProviderUsageWindow{Name: name, UsedPercent: clampPercent(100 - percent)}
		for j := i; j < len(lines) && j <= i+2; j++ {
			if reset := resetParenthetical(lines[j]); reset != "" {
				window.Reset = reset
				break
			}
		}
		usage.Windows = append(usage.Windows, window)
	}
	if len(usage.Windows) == 0 {
		usage.Available = false
		usage.Confidence = "none"
	}
	return usage
}

func normalizeProviderUsageText(output string) string {
	clean := stripANSI(output)
	clean = strings.ReplaceAll(clean, "\u00a0", " ")
	return strings.Map(func(r rune) rune {
		if r == '█' || r == '▌' || r == '▐' || r == '▛' || r == '▜' || r == '▝' || r == '▘' || r == '▗' || r == '▖' || r == '─' || r == '│' || r == '╭' || r == '╮' || r == '╰' || r == '╯' {
			return ' '
		}
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return ' '
		}
		return r
	}, clean)
}

func nonEmptyUsageLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.Join(strings.Fields(line), " ")
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

func stripANSI(text string) string {
	return ansiPattern.ReplaceAllString(text, "")
}

func percentBeforeWord(line string, word string) (int, bool) {
	re := regexp.MustCompile(`(\d{1,3})%\s+` + regexp.QuoteMeta(word) + `\b`)
	match := re.FindStringSubmatch(line)
	if len(match) != 2 {
		return 0, false
	}
	value, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return clampPercent(value), true
}

func resetParenthetical(line string) string {
	start := strings.Index(line, "(resets ")
	if start < 0 {
		return ""
	}
	rest := line[start+len("(resets "):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rest[:end])
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func providerUsagePressure(usage ProviderUsage) (int, bool) {
	if !usage.Available || len(usage.Windows) == 0 {
		return 0, false
	}
	maxUsed := 0
	for _, window := range usage.Windows {
		if window.UsedPercent > maxUsed {
			maxUsed = window.UsedPercent
		}
	}
	return clampPercent(maxUsed), true
}

func classifyProviderUsageExhaustion(kind string, values ...string) (ProviderUsageExhaustion, bool) {
	text := strings.TrimSpace(strings.Join(values, "\n"))
	if text == "" {
		return ProviderUsageExhaustion{}, false
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(text), " "))
	for _, excluded := range []string{
		"context window",
		"context-window",
		"context_window",
		"model_context_window",
		"maximum context",
		"too many tokens",
		"token limit exceeded",
	} {
		if strings.Contains(normalized, excluded) {
			return ProviderUsageExhaustion{}, false
		}
	}

	provider := strings.TrimSpace(kind)
	switch {
	case strings.Contains(normalized, "claude"):
		provider = "claude"
	case strings.Contains(normalized, "codex") || strings.Contains(normalized, "openai"):
		provider = "codex"
	}

	needles := []string{
		"usage limit",
		"usage limits",
		"limit reached",
		"rate limit",
		"rate_limit",
		"rate_limit_exceeded",
		"too many requests",
		"status 429",
		"429 too many",
		"quota exceeded",
		"insufficient_quota",
		"current quota",
		"credit balance",
		"monthly spend limit",
		"usage credits",
		"try again later",
		"limits reset",
		"limit resets",
	}
	matched := false
	for _, needle := range needles {
		if strings.Contains(normalized, needle) {
			matched = true
			break
		}
	}
	if !matched {
		return ProviderUsageExhaustion{}, false
	}
	summary := "Model provider usage is exhausted."
	if provider != "" {
		summary = provider + " usage is exhausted."
	}
	return ProviderUsageExhaustion{
		Provider: provider,
		Summary:  summary,
		Detail:   truncateStringForPrompt(text, 2000),
	}, true
}
