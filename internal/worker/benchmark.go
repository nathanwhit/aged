package worker

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type BenchmarkCompareRunner struct{}

func (BenchmarkCompareRunner) Kind() string {
	return "benchmark_compare"
}

func (BenchmarkCompareRunner) BuildCommand(Spec) []string {
	return nil
}

func (BenchmarkCompareRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	input := parseBenchmarkInput(spec.Prompt)
	if input.Baseline == 0 {
		return fmt.Errorf("benchmark_compare requires a non-zero baseline value")
	}
	deltaPercent := ((input.Candidate - input.Baseline) / math.Abs(input.Baseline)) * 100
	improved := deltaPercent >= input.ThresholdPercent
	if !input.HigherIsBetter {
		deltaPercent = ((input.Baseline - input.Candidate) / math.Abs(input.Baseline)) * 100
		improved = deltaPercent >= input.ThresholdPercent
	}
	verdict := "no measurable improvement"
	if improved {
		verdict = "improved"
	}
	report := fmt.Sprintf(`# Benchmark Comparison

## Commands Run
%s

## Benchmark Results
- baseline: %.6g
- candidate: %.6g
- threshold_percent: %.6g
- delta_percent: %.6g
- higher_is_better: %t
- verdict: %s

## Recommended Next Turns
%s
`, input.Command, input.Baseline, input.Candidate, input.ThresholdPercent, deltaPercent, input.HigherIsBetter, verdict, benchmarkRecommendation(improved))
	return sink.Event(ctx, Event{Kind: EventResult, Text: report})
}

type benchmarkInput struct {
	Command          string
	Baseline         float64
	Candidate        float64
	ThresholdPercent float64
	HigherIsBetter   bool
}

func parseBenchmarkInput(prompt string) benchmarkInput {
	input := benchmarkInput{
		Command:          "not provided",
		ThresholdPercent: 0,
		HigherIsBetter:   true,
	}
	for _, line := range strings.Split(prompt, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		switch key {
		case "command", "benchmark_command":
			input.Command = value
		case "baseline", "baseline_value":
			input.Baseline = firstNumber(value)
		case "candidate", "candidate_value":
			input.Candidate = firstNumber(value)
		case "threshold_percent", "min_improvement_percent":
			input.ThresholdPercent = firstNumber(value)
		case "higher_is_better":
			input.HigherIsBetter = strings.EqualFold(value, "true") || strings.EqualFold(value, "yes") || value == "1"
		}
	}
	return input
}

var numberPattern = regexp.MustCompile(`[-+]?\d+(?:\.\d+)?`)

func firstNumber(value string) float64 {
	match := numberPattern.FindString(value)
	if match == "" {
		return 0
	}
	number, _ := strconv.ParseFloat(match, 64)
	return number
}

func benchmarkRecommendation(improved bool) string {
	if improved {
		return "Keep the candidate if validation coverage is adequate; otherwise schedule one more validation worker."
	}
	return "Reject or revise this candidate and schedule another implementation turn if the opportunity still looks promising."
}
