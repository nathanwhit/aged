package orchestrator

import (
	"errors"
	"path"
	"strings"
	"unicode"

	"aged/internal/core"
)

func resolveRemoteCheckout(project core.Project, target TargetConfig) (string, error) {
	targetID := strings.TrimSpace(target.ID)
	if project.RemoteCheckouts != nil && targetID != "" {
		if checkout := strings.TrimSpace(project.RemoteCheckouts[targetID]); checkout != "" {
			return checkout, nil
		}
	}
	root := strings.TrimSpace(targetCheckoutRoot(target))
	if root == "" {
		return "", errors.New("remote checkoutRoot is required")
	}
	projectPart := safeRemoteCheckoutProjectPath(project)
	if projectPart == "" {
		return "", errors.New("project id is required")
	}
	return path.Join(root, projectPart), nil
}

func targetCheckoutRoot(target TargetConfig) string {
	if root := strings.TrimSpace(target.CheckoutRoot); root != "" {
		return root
	}
	return strings.TrimSpace(target.WorkDir)
}

func safeRemoteCheckoutProjectPath(project core.Project) string {
	id := strings.TrimSpace(project.ID)
	if id == "" {
		id = strings.TrimSpace(project.Name)
	}
	parts := strings.FieldsFunc(id, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	for i, part := range parts {
		parts[i] = safeRemoteCheckoutSegment(part)
	}
	return strings.Join(nonEmptySegments(parts), "/")
}

func nonEmptySegments(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" && part != "." && part != ".." {
			out = append(out, part)
		}
	}
	return out
}

func safeRemoteCheckoutSegment(value string) string {
	var builder strings.Builder
	lastDash := false
	for _, r := range strings.TrimSpace(value) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), ".-")
}
