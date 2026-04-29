package orchestrator

import (
	"context"
	"strings"
	"unicode"

	"aged/internal/core"
)

type TitleGenerator interface {
	GenerateTitle(ctx context.Context, prompt string) (string, error)
}

type AssistantTitleGenerator struct {
	Assistant AssistantProvider
}

func (g AssistantTitleGenerator) GenerateTitle(ctx context.Context, prompt string) (string, error) {
	response, err := g.Assistant.Ask(ctx, core.AssistantRequest{
		Message: "Generate a short title for this development task. Return only the title, 1-8 words, no quotes, no punctuation unless necessary.\n\nTask prompt:\n" + prompt,
	})
	if err != nil {
		return "", err
	}
	return normalizeGeneratedTitle(response.Message), nil
}

func normalizeGeneratedTitle(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`\"' \t\r\n")
	value = strings.Split(value, "\n")[0]
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	words := strings.Fields(value)
	if len(words) > 8 {
		words = words[:8]
	}
	value = strings.Join(words, " ")
	value = strings.TrimFunc(value, func(r rune) bool {
		return unicode.IsPunct(r) && r != '-' && r != '/' && r != '#'
	})
	return strings.TrimSpace(value)
}

func fallbackTaskTitle(prompt string) string {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return "Untitled task"
	}
	replacer := strings.NewReplacer("\n", " ", "\t", " ", "\r", " ")
	words := strings.Fields(replacer.Replace(prompt))
	if len(words) > 6 {
		words = words[:6]
	}
	title := normalizeGeneratedTitle(strings.Join(words, " "))
	if title == "" {
		return "Untitled task"
	}
	return title
}
