package worker

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
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
	if strings.TrimSpace(input.BaselineCommand) != "" && strings.TrimSpace(input.CandidateCommand) != "" && strings.TrimSpace(input.BaselineCommand) != strings.TrimSpace(input.CandidateCommand) {
		return fmt.Errorf("benchmark_compare requires baseline_command and candidate_command to match")
	}
	if (input.BaselineTargetID == "") != (input.CandidateTargetID == "") {
		return fmt.Errorf("benchmark_compare requires baseline_target_id and candidate_target_id to both be provided when either is provided")
	}
	if input.BaselineTargetID != "" && input.BaselineTargetID != input.CandidateTargetID {
		return fmt.Errorf("benchmark_compare requires baseline_target_id and candidate_target_id to match")
	}
	if len(input.BaselineSamples) > 0 || len(input.CandidateSamples) > 0 {
		if len(input.BaselineSamples) < input.MinSamples || len(input.CandidateSamples) < input.MinSamples {
			return fmt.Errorf("benchmark_compare requires at least %d baseline and candidate samples", input.MinSamples)
		}
		input.Baseline = median(input.BaselineSamples)
		input.Candidate = median(input.CandidateSamples)
	}
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
- target_id: %s
- baseline: %.6g
- candidate: %.6g
- baseline_samples: %s
- candidate_samples: %s
- sample_count: %d
- min_samples: %d
- threshold_percent: %.6g
- delta_percent: %.6g
- higher_is_better: %t
- verdict: %s

## Recommended Next Turns
%s
`, input.Command, nonEmptyBenchmarkValue(input.BaselineTargetID, "not provided"), input.Baseline, input.Candidate, sampleSummary(input.BaselineSamples), sampleSummary(input.CandidateSamples), minNonZero(len(input.BaselineSamples), len(input.CandidateSamples)), input.MinSamples, input.ThresholdPercent, deltaPercent, input.HigherIsBetter, verdict, benchmarkRecommendation(improved))
	return sink.Event(ctx, Event{Kind: EventResult, Text: report})
}

type benchmarkInput struct {
	Command           string
	BaselineCommand   string
	CandidateCommand  string
	BaselineTargetID  string
	CandidateTargetID string
	Baseline          float64
	Candidate         float64
	BaselineSamples   []float64
	CandidateSamples  []float64
	MinSamples        int
	ThresholdPercent  float64
	HigherIsBetter    bool
}

func parseBenchmarkInput(prompt string) benchmarkInput {
	input := benchmarkInput{
		Command:          "not provided",
		ThresholdPercent: 0,
		HigherIsBetter:   true,
		MinSamples:       1,
	}
	for _, line := range strings.Split(prompt, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = benchmarkInputKey(key)
		value = strings.TrimSpace(value)
		switch key {
		case "command", "benchmark_command":
			input.Command = value
		case "baseline_command":
			input.BaselineCommand = value
			if input.Command == "not provided" {
				input.Command = value
			}
		case "candidate_command":
			input.CandidateCommand = value
		case "target_id", "benchmark_target_id":
			input.BaselineTargetID = value
			input.CandidateTargetID = value
		case "baseline_target_id":
			input.BaselineTargetID = value
		case "candidate_target_id":
			input.CandidateTargetID = value
		case "baseline", "baseline_value":
			input.Baseline = firstNumber(value)
		case "candidate", "candidate_value":
			input.Candidate = firstNumber(value)
		case "baseline_samples", "baseline_values":
			input.BaselineSamples = numbers(value)
		case "candidate_samples", "candidate_values":
			input.CandidateSamples = numbers(value)
		case "min_samples", "sample_count":
			input.MinSamples = int(firstNumber(value))
			if input.MinSamples <= 0 {
				input.MinSamples = 1
			}
		case "threshold_percent", "min_improvement_percent":
			input.ThresholdPercent = firstNumber(value)
		case "higher_is_better":
			input.HigherIsBetter = strings.EqualFold(value, "true") || strings.EqualFold(value, "yes") || value == "1"
		}
	}
	return input
}

func benchmarkInputKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, "-", "_")
	key = strings.ReplaceAll(key, " ", "_")
	switch key {
	case "target", "targetid":
		return "target_id"
	case "benchmark_target", "benchmark_targetid":
		return "benchmark_target_id"
	case "baseline_target", "baseline_targetid":
		return "baseline_target_id"
	case "candidate_target", "candidate_targetid":
		return "candidate_target_id"
	default:
		return key
	}
}

func nonEmptyBenchmarkValue(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

var numberPattern = regexp.MustCompile(`[-+]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][-+]?\d+)?`)

func firstNumber(value string) float64 {
	match := numberPattern.FindString(value)
	if match == "" {
		return 0
	}
	number, _ := strconv.ParseFloat(match, 64)
	return number
}

func numbers(value string) []float64 {
	matches := numberPattern.FindAllString(value, -1)
	out := make([]float64, 0, len(matches))
	for _, match := range matches {
		number, err := strconv.ParseFloat(match, 64)
		if err == nil {
			out = append(out, number)
		}
	}
	return out
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64{}, values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func sampleSummary(values []float64) string {
	if len(values) == 0 {
		return "not provided"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(value, 'g', 6, 64))
	}
	return strings.Join(parts, ", ")
}

func minNonZero(a int, b int) int {
	if a == 0 || b == 0 {
		if a > b {
			return a
		}
		return b
	}
	if a < b {
		return a
	}
	return b
}

func benchmarkRecommendation(improved bool) string {
	if improved {
		return "Keep the candidate if validation coverage is adequate; otherwise schedule one more validation worker."
	}
	return "Reject or revise this candidate and schedule another implementation turn if the opportunity still looks promising."
}
