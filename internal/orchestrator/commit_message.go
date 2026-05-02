package orchestrator

import (
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

type changeCommitMessageContext struct {
	Fallback      string
	TaskTitle     string
	WorkerSummary string
	PullTitle     string
	Metadata      map[string]any
	ChangedFiles  []string
}

func changeCommitMessage(ctx changeCommitMessageContext) string {
	for _, candidate := range []string{
		ctx.WorkerSummary,
		ctx.PullTitle,
		ctx.TaskTitle,
		stringMetadataValue(ctx.Metadata["summary"]),
		stringMetadataValue(ctx.Metadata["title"]),
		stringMetadataValue(ctx.Metadata["taskTitle"]),
		stringMetadataValue(ctx.Metadata["message"]),
	} {
		if title := normalizeCommitMessageTitle(candidate); title != "" && !isGenericCommitMessageTitle(title) {
			return title
		}
	}
	if title := commitMessageFromChangedFiles(ctx.ChangedFiles); title != "" {
		return title
	}
	return ctx.Fallback
}

func normalizeCommitMessageTitle(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, line := range strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n") {
		if content, ok := workerReportHeadingInlineContent(line); ok {
			line = content
		}
		title := normalizeCommitMessageTitleLine(line)
		if title == "" || isWorkerReportHeadingTitle(title) {
			continue
		}
		return title
	}
	return ""
}

func normalizeCommitMessageTitleLine(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, "#*- \t")
	value = strings.Trim(value, "`\"' \t\r\n")
	value = strings.Join(strings.Fields(value), " ")
	value = strings.TrimFunc(value, func(r rune) bool {
		return unicode.IsPunct(r) && r != '-' && r != '/' && r != '#'
	})
	if value == "" {
		return ""
	}
	const maxRunes = 72
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	trimmed := string(runes[:maxRunes])
	if lastSpace := strings.LastIndex(trimmed, " "); lastSpace >= 24 {
		trimmed = trimmed[:lastSpace]
	}
	return strings.TrimSpace(trimmed)
}

func workerReportHeadingInlineContent(value string) (string, bool) {
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, "> \t")
	value = strings.TrimLeft(value, "# \t")
	value = strings.TrimLeft(value, "-+ \t")
	value = strings.Trim(value, "`\"' \t\r\n")
	value = strings.Trim(value, "*_ ")
	label, content, ok := strings.Cut(value, ":")
	if !ok || !isWorkerReportHeadingTitle(label) {
		return "", false
	}
	return strings.TrimSpace(content), true
}

func isWorkerReportHeadingTitle(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(value), " "))
	normalized = strings.Trim(normalized, " .:-_#*`")
	switch normalized {
	case "findings",
		"summary",
		"change summary",
		"implementation summary",
		"commands",
		"command run",
		"commands run",
		"tests",
		"testing",
		"benchmark results",
		"changed files",
		"files changed",
		"file changes",
		"blockers",
		"recommended next turns",
		"recommendations",
		"next steps",
		"open questions",
		"assumptions",
		"notes":
		return true
	default:
		return false
	}
}

func isGenericCommitMessageTitle(value string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(value), " "))
	normalized = strings.Trim(normalized, " .:-_")
	switch normalized {
	case "", "ci", "fix", "published", "publish", "changes", "change", "updates", "update", "work", "base worker candidate", "publish aged worker changes":
		return true
	default:
		return false
	}
}

func commitMessageFromChangedFiles(files []string) string {
	paths := cleanedCommitMessagePaths(files)
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return "Update " + commitMessagePathSubject(paths[0])
	}
	if common := commonCommitMessageDir(paths); common != "" && common != "." {
		return "Update " + commitMessagePathSubject(common)
	}
	first := commitMessagePathSubject(paths[0])
	if len(paths) == 2 {
		return "Update " + first + " and 1 other file"
	}
	return "Update " + first + " and " + strconv.Itoa(len(paths)-1) + " other files"
}

func cleanedCommitMessagePaths(files []string) []string {
	seen := map[string]bool{}
	paths := []string{}
	for _, file := range files {
		file = strings.TrimSpace(filepath.ToSlash(file))
		file = strings.Trim(file, "/")
		if file == "" || seen[file] {
			continue
		}
		seen[file] = true
		paths = append(paths, file)
	}
	return paths
}

func commonCommitMessageDir(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	common := filepath.ToSlash(filepath.Dir(paths[0]))
	for _, path := range paths[1:] {
		dir := filepath.ToSlash(filepath.Dir(path))
		for common != "." && common != "" && dir != common && !strings.HasPrefix(dir, common+"/") {
			common = filepath.ToSlash(filepath.Dir(common))
		}
		if common == "." || common == "" {
			return ""
		}
	}
	return common
}

func commitMessagePathSubject(path string) string {
	path = strings.Trim(strings.TrimSpace(filepath.ToSlash(path)), "/")
	switch {
	case path == "":
		return ""
	case path == ".github/workflows" || strings.HasPrefix(path, ".github/workflows/"):
		return "GitHub workflows"
	case path == ".github":
		return "GitHub configuration"
	case path == "go.mod" || path == "go.sum":
		return "Go module metadata"
	case path == "package.json" || path == "package-lock.json":
		return "Node package metadata"
	}
	parts := strings.Split(path, "/")
	if len(parts) > 1 {
		path = strings.Join(parts[:len(parts)-1], " ")
	} else {
		path = strings.TrimSuffix(parts[0], filepath.Ext(parts[0]))
	}
	replacer := strings.NewReplacer("_", " ", "-", " ", ".", " ")
	subject := strings.Join(strings.Fields(replacer.Replace(path)), " ")
	if subject == "" {
		return path
	}
	return subject
}
