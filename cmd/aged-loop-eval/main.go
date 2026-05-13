package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"aged/internal/core"
	"aged/internal/envutil"
)

const evalSource = "aged-loop-eval"

type config struct {
	baseURL       string
	evalPath      string
	outputPath    string
	outputSet     bool
	title         string
	horizon       time.Duration
	poll          time.Duration
	cancel        bool
	steerAfter    time.Duration
	steering      string
	workerKind    string
	loopInterval  int
	repeat        time.Duration
	maxRuns       int
	staleAfter    time.Duration
	feedback      bool
	feedbackTitle string
}

type evalDefinition struct {
	Prompt   string         `json:"prompt"`
	Metadata map[string]any `json:"metadata"`
}

type evalResult struct {
	Name                 string             `json:"name"`
	TaskID               string             `json:"taskId"`
	StartedAt            time.Time          `json:"startedAt"`
	EndedAt              time.Time          `json:"endedAt"`
	HorizonSeconds       float64            `json:"horizonSeconds"`
	StaleWorkerAfterSec  float64            `json:"staleWorkerAfterSeconds,omitempty"`
	BaseURL              string             `json:"baseUrl"`
	EvalPath             string             `json:"evalPath"`
	OutputPath           string             `json:"outputPath"`
	TaskStatusBeforeStop core.TaskStatus    `json:"taskStatusBeforeStop"`
	FinalTaskStatus      core.TaskStatus    `json:"finalTaskStatus"`
	CanceledByRunner     bool               `json:"canceledByRunner"`
	SteeringSent         bool               `json:"steeringSent"`
	SteeringMessage      string             `json:"steeringMessage,omitempty"`
	Metrics              evalMetrics        `json:"metrics"`
	Checks               []evalCheck        `json:"checks"`
	PullRequests         []pullRequestScore `json:"pullRequests,omitempty"`
	EventsSample         []eventSample      `json:"eventsSample,omitempty"`
	FeedbackCreated      bool               `json:"feedbackCreated"`
	FeedbackTaskID       string             `json:"feedbackTaskId,omitempty"`
	FeedbackError        string             `json:"feedbackError,omitempty"`
}

type evalMetrics struct {
	IterationsCompleted              int        `json:"iterationsCompleted"`
	IterationsFailed                 int        `json:"iterationsFailed"`
	IterationsCanceled               int        `json:"iterationsCanceled"`
	LoopPlans                        int        `json:"loopPlans"`
	WorkersCreated                   int        `json:"workersCreated"`
	WorkersSucceeded                 int        `json:"workersSucceeded"`
	WorkersFailed                    int        `json:"workersFailed"`
	WorkersCanceled                  int        `json:"workersCanceled"`
	WorkerNeedsInput                 int        `json:"workerNeedsInput"`
	PullRequestsTracked              int        `json:"pullRequestsTracked"`
	PullRequestFollowUps             int        `json:"pullRequestFollowUps"`
	EmptyOrNoDiffPullRequests        int        `json:"emptyOrNoDiffPullRequests"`
	TaskWaitingTransitions           int        `json:"taskWaitingTransitions"`
	MinutesToFirstPullRequest        *float64   `json:"minutesToFirstPullRequest,omitempty"`
	SecondsFromSteeringToNextWorker  *float64   `json:"secondsFromSteeringToNextWorker,omitempty"`
	LatestWorkerOutputAt             *time.Time `json:"latestWorkerOutputAt,omitempty"`
	SecondsSinceLatestWorkerOutput   *float64   `json:"secondsSinceLatestWorkerOutput,omitempty"`
	StaleRunningWorkers              int        `json:"staleRunningWorkers"`
	MaxRunningWorkerSilenceSeconds   *float64   `json:"maxRunningWorkerSilenceSeconds,omitempty"`
	RepositoryInspectionEventCount   int        `json:"repositoryInspectionEventCount"`
	TestCommandEventCount            int        `json:"testCommandEventCount"`
	MaterialPullRequestEvidenceCount int        `json:"materialPullRequestEvidenceCount"`
}

type evalCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type pullRequestScore struct {
	ID           string `json:"id"`
	Repo         string `json:"repo"`
	Number       int    `json:"number,omitempty"`
	URL          string `json:"url"`
	Title        string `json:"title"`
	State        string `json:"state,omitempty"`
	ChecksStatus string `json:"checksStatus,omitempty"`
	ReviewStatus string `json:"reviewStatus,omitempty"`
	ChangedFiles int    `json:"changedFiles,omitempty"`
	BodyQuality  string `json:"bodyQuality,omitempty"`
	GitHubError  string `json:"githubError,omitempty"`
}

type eventSample struct {
	ID       int64          `json:"id"`
	At       time.Time      `json:"at"`
	Type     core.EventType `json:"type"`
	WorkerID string         `json:"workerId,omitempty"`
	Summary  string         `json:"summary,omitempty"`
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, "aged-loop-eval:", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.baseURL, "addr", envutil.TrimmedString("AGED_LOOP_EVAL_ADDR", "http://127.0.0.1:8787"), "aged daemon base URL")
	flag.StringVar(&cfg.evalPath, "eval", envutil.TrimmedString("AGED_LOOP_EVAL_PATH", "evals/durable-loop-pr-producer.md"), "eval markdown file")
	flag.StringVar(&cfg.outputPath, "out", envutil.TrimmedString("AGED_LOOP_EVAL_OUT", ""), "scorecard JSON output path")
	flag.StringVar(&cfg.title, "title", envutil.TrimmedString("AGED_LOOP_EVAL_TITLE", "Durable loop PR producer eval"), "task title")
	flag.DurationVar(&cfg.horizon, "horizon", envutil.Duration("AGED_LOOP_EVAL_HORIZON", 90*time.Minute), "external eval horizon before canceling/scoring")
	flag.DurationVar(&cfg.poll, "poll", envutil.Duration("AGED_LOOP_EVAL_POLL", 10*time.Second), "snapshot polling interval")
	flag.BoolVar(&cfg.cancel, "cancel", envutil.Bool("AGED_LOOP_EVAL_CANCEL", true), "cancel the task when the horizon expires")
	flag.DurationVar(&cfg.steerAfter, "steer-after", envutil.Duration("AGED_LOOP_EVAL_STEER_AFTER", 0), "send steering after this delay; 0 disables")
	flag.StringVar(&cfg.steering, "steering", envutil.TrimmedString("AGED_LOOP_EVAL_STEERING", "Keep the next change narrow and check the existing PR state before opening anything new."), "steering message")
	flag.StringVar(&cfg.workerKind, "worker-kind", envutil.TrimmedString("AGED_LOOP_EVAL_WORKER_KIND", ""), "override metadata.loopWorkerKind for smoke runs")
	flag.IntVar(&cfg.loopInterval, "loop-interval-seconds", envutil.Int("AGED_LOOP_EVAL_LOOP_INTERVAL_SECONDS", -1), "override metadata.loopIntervalSeconds; -1 keeps eval metadata")
	flag.DurationVar(&cfg.repeat, "repeat", envutil.Duration("AGED_LOOP_EVAL_REPEAT", 0), "delay between eval runs; 0 runs once")
	flag.IntVar(&cfg.maxRuns, "max-runs", envutil.Int("AGED_LOOP_EVAL_MAX_RUNS", 1), "maximum eval runs; 0 means forever when -repeat is set")
	flag.DurationVar(&cfg.staleAfter, "stale-worker-after", envutil.Duration("AGED_LOOP_EVAL_STALE_WORKER_AFTER", 15*time.Minute), "fail the scorecard when a nonterminal worker has no activity for this long; 0 disables")
	flag.BoolVar(&cfg.feedback, "feedback-on-fail", envutil.Bool("AGED_LOOP_EVAL_FEEDBACK_ON_FAIL", false), "create a follow-up aged improvement task when any scorecard check fails")
	flag.StringVar(&cfg.feedbackTitle, "feedback-title", envutil.TrimmedString("AGED_LOOP_EVAL_FEEDBACK_TITLE", "Improve durable loop eval result"), "title for feedback tasks created by -feedback-on-fail")
	flag.Parse()
	cfg.baseURL = strings.TrimRight(strings.TrimSpace(cfg.baseURL), "/")
	cfg.outputSet = strings.TrimSpace(cfg.outputPath) != ""
	if cfg.poll <= 0 {
		cfg.poll = time.Second
	}
	if cfg.horizon <= 0 {
		cfg.horizon = time.Minute
	}
	return cfg
}

func run(ctx context.Context, cfg config) error {
	runs := 0
	for {
		runs++
		result, err := runOnce(ctx, cfg, runs)
		if err != nil {
			return err
		}
		if cfg.feedback && resultHasFailingChecks(result) {
			task, err := createFeedbackTask(ctx, cfg, result)
			if err != nil {
				result.FeedbackError = err.Error()
			} else {
				result.FeedbackCreated = true
				result.FeedbackTaskID = task.ID
				fmt.Printf("created feedback task %s for eval task %s\n", task.ID, result.TaskID)
			}
		}
		if err := writeResult(result); err != nil {
			return err
		}
		fmt.Printf("wrote eval scorecard %s\n", result.OutputPath)
		printSummary(result)
		if cfg.feedback && result.FeedbackError != "" {
			fmt.Printf("feedback task creation failed: %s\n", result.FeedbackError)
		}
		if cfg.repeat <= 0 || (cfg.maxRuns > 0 && runs >= cfg.maxRuns) {
			return nil
		}
		timer := time.NewTimer(cfg.repeat)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func runOnce(ctx context.Context, cfg config, runIndex int) (evalResult, error) {
	definition, err := loadEvalDefinition(cfg.evalPath)
	if err != nil {
		return evalResult{}, err
	}
	if strings.TrimSpace(cfg.workerKind) != "" {
		definition.Metadata["loopWorkerKind"] = strings.TrimSpace(cfg.workerKind)
	}
	if cfg.loopInterval >= 0 {
		definition.Metadata["loopIntervalSeconds"] = cfg.loopInterval
	}
	metadata, err := json.Marshal(definition.Metadata)
	if err != nil {
		return evalResult{}, err
	}
	started := time.Now().UTC()
	cfg.outputPath = outputPathForRun(cfg, started, runIndex)
	task, err := createTask(ctx, cfg, definition.Prompt, metadata, started)
	if err != nil {
		return evalResult{}, err
	}
	fmt.Printf("created eval task %s\n", task.ID)

	deadline := time.Now().Add(cfg.horizon)
	var preStop core.Snapshot
	var steeringAt *time.Time
	steered := false
	for {
		snapshot, err := getSnapshot(ctx, cfg.baseURL)
		if err != nil {
			return evalResult{}, err
		}
		preStop = snapshot
		if shouldSteer(started, cfg, steered) {
			if err := steerTask(ctx, cfg.baseURL, task.ID, cfg.steering); err != nil {
				return evalResult{}, err
			}
			now := time.Now().UTC()
			steeringAt = &now
			steered = true
			fmt.Printf("sent steering to task %s\n", task.ID)
		}
		if time.Now().After(deadline) || terminalTaskStatus(taskStatus(snapshot, task.ID)) {
			break
		}
		time.Sleep(cfg.poll)
	}

	canceled := false
	if cfg.cancel && !terminalTaskStatus(taskStatus(preStop, task.ID)) {
		if err := cancelTask(ctx, cfg.baseURL, task.ID); err != nil {
			return evalResult{}, err
		}
		canceled = true
		fmt.Printf("canceled eval task %s at external horizon\n", task.ID)
	}
	finalSnapshot := waitForSettledSnapshot(ctx, cfg, task.ID)
	events, err := getTaskEvents(ctx, cfg.baseURL, task.ID, 1000)
	if err != nil {
		return evalResult{}, err
	}
	return buildResult(cfg, task.ID, started, preStop, finalSnapshot, events, canceled, steered, steeringAt), nil
}

func loadEvalDefinition(path string) (evalDefinition, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return evalDefinition{}, err
	}
	prompt, err := fencedBlockAfterHeading(string(content), "## Task Prompt")
	if err != nil {
		return evalDefinition{}, err
	}
	metadataBlock, err := fencedBlockAfterHeading(string(content), "## Metadata")
	if err != nil {
		return evalDefinition{}, err
	}
	metadata := map[string]any{}
	if err := json.Unmarshal([]byte(metadataBlock), &metadata); err != nil {
		return evalDefinition{}, fmt.Errorf("parse metadata block: %w", err)
	}
	return evalDefinition{Prompt: prompt, Metadata: metadata}, nil
}

func fencedBlockAfterHeading(content string, heading string) (string, error) {
	index := strings.Index(content, heading)
	if index < 0 {
		return "", fmt.Errorf("missing heading %q", heading)
	}
	rest := content[index+len(heading):]
	start := strings.Index(rest, "```")
	if start < 0 {
		return "", fmt.Errorf("missing fenced block after %q", heading)
	}
	rest = rest[start+3:]
	if newline := strings.Index(rest, "\n"); newline >= 0 {
		rest = rest[newline+1:]
	}
	end := strings.Index(rest, "```")
	if end < 0 {
		return "", fmt.Errorf("unterminated fenced block after %q", heading)
	}
	return strings.TrimSpace(rest[:end]), nil
}

func createTask(ctx context.Context, cfg config, prompt string, metadata json.RawMessage, started time.Time) (core.Task, error) {
	req := core.CreateTaskRequest{
		Title:      cfg.title,
		Prompt:     prompt,
		Source:     evalSource,
		ExternalID: "durable-loop-pr-producer-" + started.Format("20060102T150405Z"),
		Metadata:   metadata,
	}
	var task core.Task
	err := postJSON(ctx, cfg.baseURL+"/api/tasks", req, http.StatusAccepted, &task)
	return task, err
}

func createFeedbackTask(ctx context.Context, cfg config, result evalResult) (core.Task, error) {
	failed := failedChecks(result)
	metadata, err := json.Marshal(map[string]any{
		"completionMode": "github",
		"eval":           result.Name,
		"evalTaskId":     result.TaskID,
		"evalResultPath": result.OutputPath,
		"failedChecks":   failed,
	})
	if err != nil {
		return core.Task{}, err
	}
	req := core.CreateTaskRequest{
		Title:      cfg.feedbackTitle,
		Prompt:     feedbackPrompt(result, failed),
		Source:     evalSource + "-feedback",
		ExternalID: result.Name + "-feedback-" + result.StartedAt.Format("20060102T150405Z"),
		Metadata:   metadata,
	}
	var task core.Task
	err = postJSON(ctx, cfg.baseURL+"/api/tasks", req, http.StatusAccepted, &task)
	return task, err
}

func feedbackPrompt(result evalResult, failed []string) string {
	var failedChecks strings.Builder
	for _, name := range failed {
		failedChecks.WriteString("- ")
		failedChecks.WriteString(name)
		failedChecks.WriteString("\n")
	}
	summary, _ := json.MarshalIndent(struct {
		Metrics evalMetrics `json:"metrics"`
		Checks  []evalCheck `json:"checks"`
	}{Metrics: result.Metrics, Checks: result.Checks}, "", "  ")
	return fmt.Sprintf(`A durable-loop eval run produced failing checks. Inspect the scorecard, decide which failures are legitimate product or evaluator problems, and implement one narrow improvement. Open a PR only if there is a real code or documentation change.

Eval: %s
Eval task ID: %s
Scorecard path: %s
Task status before evaluator stop: %s
Final task status: %s

Failed checks:
%s
Scorecard summary:
%s

Do not blindly optimize for the smoke worker. If a failure is expected for mock mode, improve the evaluator or documentation so real failures and expected smoke failures are distinguishable.`, result.Name, result.TaskID, result.OutputPath, result.TaskStatusBeforeStop, result.FinalTaskStatus, failedChecks.String(), summary)
}

func getSnapshot(ctx context.Context, baseURL string) (core.Snapshot, error) {
	var snapshot core.Snapshot
	err := getJSON(ctx, baseURL+"/api/snapshot", &snapshot)
	return snapshot, err
}

func getTaskEvents(ctx context.Context, baseURL string, taskID string, limit int) ([]core.Event, error) {
	var events []core.Event
	err := getJSON(ctx, fmt.Sprintf("%s/api/tasks/%s/events?limit=%d", baseURL, taskID, limit), &events)
	return events, err
}

func steerTask(ctx context.Context, baseURL string, taskID string, message string) error {
	return postJSON(ctx, baseURL+"/api/tasks/"+taskID+"/steer", core.SteeringRequest{Message: message}, http.StatusNoContent, nil)
}

func cancelTask(ctx context.Context, baseURL string, taskID string) error {
	return postJSON(ctx, baseURL+"/api/tasks/"+taskID+"/cancel", nil, http.StatusNoContent, nil)
}

func getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("GET %s: %s: %s", url, res.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func postJSON(ctx context.Context, url string, in any, expected int, out any) error {
	var body io.Reader
	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != expected {
		response, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("POST %s: %s: %s", url, res.Status, strings.TrimSpace(string(response)))
	}
	if out == nil || res.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func waitForSettledSnapshot(ctx context.Context, cfg config, taskID string) core.Snapshot {
	deadline := time.Now().Add(30 * time.Second)
	var latest core.Snapshot
	for time.Now().Before(deadline) {
		snapshot, err := getSnapshot(ctx, cfg.baseURL)
		if err == nil {
			latest = snapshot
			if terminalTaskStatus(taskStatus(snapshot, taskID)) {
				return snapshot
			}
		}
		time.Sleep(cfg.poll)
	}
	return latest
}

func shouldSteer(started time.Time, cfg config, steered bool) bool {
	return !steered && cfg.steerAfter > 0 && time.Since(started) >= cfg.steerAfter
}

func buildResult(cfg config, taskID string, started time.Time, preStop core.Snapshot, finalSnapshot core.Snapshot, events []core.Event, canceled bool, steered bool, steeringAt *time.Time) evalResult {
	ended := time.Now().UTC()
	metrics := collectMetrics(taskID, preStop, events, ended, steeringAt, cfg.staleAfter)
	prs := scorePullRequests(taskID, preStop.PullRequests)
	for _, pr := range prs {
		if pr.ChangedFiles > 0 {
			metrics.MaterialPullRequestEvidenceCount++
		}
		if pr.Number != 0 && pr.ChangedFiles == 0 && pr.GitHubError == "" {
			metrics.EmptyOrNoDiffPullRequests++
		}
	}
	result := evalResult{
		Name:                 "durable-loop-pr-producer",
		TaskID:               taskID,
		StartedAt:            started,
		EndedAt:              ended,
		HorizonSeconds:       cfg.horizon.Seconds(),
		StaleWorkerAfterSec:  staleWorkerAfterSeconds(cfg.staleAfter),
		BaseURL:              cfg.baseURL,
		EvalPath:             cfg.evalPath,
		OutputPath:           cfg.outputPath,
		TaskStatusBeforeStop: taskStatus(preStop, taskID),
		FinalTaskStatus:      taskStatus(finalSnapshot, taskID),
		CanceledByRunner:     canceled,
		SteeringSent:         steered,
		SteeringMessage:      steeringMessage(steered, cfg.steering),
		Metrics:              metrics,
		PullRequests:         prs,
		EventsSample:         sampleEvents(events, 25),
	}
	result.Checks = scoreChecks(result)
	return result
}

func collectMetrics(taskID string, snapshot core.Snapshot, events []core.Event, ended time.Time, steeringAt *time.Time, staleAfter time.Duration) evalMetrics {
	var metrics evalMetrics
	firstPRAt := (*time.Time)(nil)
	nextWorkerAfterSteering := (*time.Time)(nil)
	for _, event := range events {
		payload := decodePayload(event.Payload)
		switch event.Type {
		case core.EventTaskPlanned:
			if payloadNestedString(payload, "metadata", "executionMode") == "loop" {
				metrics.LoopPlans++
			}
		case core.EventTaskStatus:
			if payloadString(payload, "status") == string(core.TaskWaiting) {
				metrics.TaskWaitingTransitions++
			}
		case core.EventTaskAction:
			if payloadString(payload, "kind") != "durable_loop" {
				continue
			}
			switch payloadString(payload, "status") {
			case "iteration_completed":
				metrics.IterationsCompleted++
			case "iteration_failed":
				metrics.IterationsFailed++
			case "iteration_canceled":
				metrics.IterationsCanceled++
			}
		case core.EventWorkerCreated:
			metrics.WorkersCreated++
			if steeringAt != nil && event.At.After(*steeringAt) && nextWorkerAfterSteering == nil {
				at := event.At
				nextWorkerAfterSteering = &at
			}
		case core.EventWorkerCompleted:
			switch core.WorkerStatus(payloadString(payload, "status")) {
			case core.WorkerSucceeded:
				metrics.WorkersSucceeded++
			case core.WorkerFailed:
				metrics.WorkersFailed++
			case core.WorkerCanceled:
				metrics.WorkersCanceled++
			case core.WorkerWaiting:
				metrics.WorkerNeedsInput++
			}
		case core.EventWorkerOutput:
			text := strings.ToLower(payloadString(payload, "text"))
			if looksLikeRepositoryInspection(text) {
				metrics.RepositoryInspectionEventCount++
			}
			if looksLikeTestCommand(text) {
				metrics.TestCommandEventCount++
			}
			at := event.At
			metrics.LatestWorkerOutputAt = &at
		case core.EventPRPublished:
			if firstPRAt == nil {
				at := event.At
				firstPRAt = &at
			}
		case core.EventPRFollowUp:
			metrics.PullRequestFollowUps++
		}
	}
	for _, pr := range snapshot.PullRequests {
		if pr.TaskID == taskID {
			metrics.PullRequestsTracked++
		}
	}
	collectRunningWorkerSilence(taskID, snapshot.Workers, ended, staleAfter, &metrics)
	if firstPRAt != nil {
		value := firstPRAt.Sub(firstEventTime(events)).Minutes()
		metrics.MinutesToFirstPullRequest = &value
	}
	if steeringAt != nil && nextWorkerAfterSteering != nil {
		value := nextWorkerAfterSteering.Sub(*steeringAt).Seconds()
		metrics.SecondsFromSteeringToNextWorker = &value
	}
	if metrics.LatestWorkerOutputAt != nil {
		value := ended.Sub(*metrics.LatestWorkerOutputAt).Seconds()
		metrics.SecondsSinceLatestWorkerOutput = &value
	}
	return metrics
}

func collectRunningWorkerSilence(taskID string, workers []core.Worker, ended time.Time, staleAfter time.Duration, metrics *evalMetrics) {
	if staleAfter <= 0 {
		return
	}
	for _, worker := range workers {
		if worker.TaskID != taskID || isTerminalEvalWorkerStatus(worker.Status) {
			continue
		}
		activityAt := worker.UpdatedAt
		if activityAt.IsZero() {
			activityAt = worker.CreatedAt
		}
		if activityAt.IsZero() || activityAt.After(ended) {
			continue
		}
		silence := ended.Sub(activityAt)
		value := silence.Seconds()
		if metrics.MaxRunningWorkerSilenceSeconds == nil || value > *metrics.MaxRunningWorkerSilenceSeconds {
			metrics.MaxRunningWorkerSilenceSeconds = &value
		}
		if silence >= staleAfter {
			metrics.StaleRunningWorkers++
		}
	}
}

func isTerminalEvalWorkerStatus(status core.WorkerStatus) bool {
	return status == core.WorkerSucceeded || status == core.WorkerFailed || status == core.WorkerCanceled
}

func scorePullRequests(taskID string, prs []core.PullRequest) []pullRequestScore {
	var out []pullRequestScore
	for _, pr := range prs {
		if pr.TaskID != taskID {
			continue
		}
		score := pullRequestScore{
			ID:           pr.ID,
			Repo:         pr.Repo,
			Number:       pr.Number,
			URL:          pr.URL,
			Title:        pr.Title,
			State:        pr.State,
			ChecksStatus: pr.ChecksStatus,
			ReviewStatus: pr.ReviewStatus,
		}
		if pr.Repo != "" && pr.Number > 0 {
			changed, bodyQuality, err := inspectPullRequestWithGH(pr.Repo, pr.Number)
			score.ChangedFiles = changed
			score.BodyQuality = bodyQuality
			if err != nil {
				score.GitHubError = err.Error()
			}
		}
		out = append(out, score)
	}
	return out
}

func inspectPullRequestWithGH(repo string, number int) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", "pr", "view", fmt.Sprint(number), "--repo", repo, "--json", "body,changedFiles")
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return 0, "unknown", fmt.Errorf("gh pr view: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		return 0, "unknown", err
	}
	var payload struct {
		Body         string `json:"body"`
		ChangedFiles int    `json:"changedFiles"`
	}
	if err := json.Unmarshal(output, &payload); err != nil {
		return 0, "unknown", err
	}
	return payload.ChangedFiles, prBodyQuality(payload.Body), nil
}

func scoreChecks(result evalResult) []evalCheck {
	statusBeforeStop := result.TaskStatusBeforeStop
	return []evalCheck{
		check("task_did_not_self_complete", !terminalTaskStatus(statusBeforeStop), fmt.Sprintf("status before evaluator stop was %q", statusBeforeStop)),
		check("opened_or_tracked_pr", result.Metrics.PullRequestsTracked > 0, fmt.Sprintf("tracked PRs: %d", result.Metrics.PullRequestsTracked)),
		check("material_pr_diff", result.Metrics.MaterialPullRequestEvidenceCount > 0, fmt.Sprintf("PRs with changed-file evidence: %d", result.Metrics.MaterialPullRequestEvidenceCount)),
		check("no_empty_pr", result.Metrics.EmptyOrNoDiffPullRequests == 0, fmt.Sprintf("empty/no-diff PRs: %d", result.Metrics.EmptyOrNoDiffPullRequests)),
		check("inspected_existing_pr_or_repo", result.Metrics.RepositoryInspectionEventCount > 0, fmt.Sprintf("inspection events: %d", result.Metrics.RepositoryInspectionEventCount)),
		check("ran_or_reported_tests", result.Metrics.TestCommandEventCount > 0, fmt.Sprintf("test command events: %d", result.Metrics.TestCommandEventCount)),
		check("cancel_did_not_complete_task", !(result.CanceledByRunner && result.FinalTaskStatus == core.TaskSucceeded), fmt.Sprintf("final status: %q", result.FinalTaskStatus)),
		check("steering_reached_next_worker", !result.SteeringSent || result.Metrics.SecondsFromSteeringToNextWorker != nil, steeringCheckReason(result)),
		check("no_stale_running_workers", result.StaleWorkerAfterSec <= 0 || result.Metrics.StaleRunningWorkers == 0, staleWorkerCheckReason(result)),
		check("no_iteration_failures", result.Metrics.IterationsFailed == 0, fmt.Sprintf("failed iterations: %d", result.Metrics.IterationsFailed)),
		check("loop_made_progress", result.Metrics.IterationsCompleted > 0 || result.Metrics.PullRequestsTracked > 0 || result.Metrics.WorkerNeedsInput > 0, fmt.Sprintf("completedIterations=%d prs=%d waitingWorkers=%d", result.Metrics.IterationsCompleted, result.Metrics.PullRequestsTracked, result.Metrics.WorkerNeedsInput)),
	}
}

func staleWorkerCheckReason(result evalResult) string {
	if result.StaleWorkerAfterSec <= 0 {
		return "stale worker detection disabled"
	}
	if result.Metrics.StaleRunningWorkers == 0 {
		return fmt.Sprintf("no nonterminal workers silent for %.0fs", result.StaleWorkerAfterSec)
	}
	return fmt.Sprintf("stale workers: %d, max silence %.0fs, threshold %.0fs", result.Metrics.StaleRunningWorkers, optionalSeconds(result.Metrics.MaxRunningWorkerSilenceSeconds), result.StaleWorkerAfterSec)
}

func optionalSeconds(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func staleWorkerAfterSeconds(value time.Duration) float64 {
	if value <= 0 {
		return 0
	}
	return value.Seconds()
}

func resultHasFailingChecks(result evalResult) bool {
	for _, check := range result.Checks {
		if check.Status == "fail" {
			return true
		}
	}
	return false
}

func failedChecks(result evalResult) []string {
	var failed []string
	for _, check := range result.Checks {
		if check.Status == "fail" {
			failed = append(failed, check.Name)
		}
	}
	return failed
}

func check(name string, pass bool, reason string) evalCheck {
	status := "fail"
	if pass {
		status = "pass"
	}
	return evalCheck{Name: name, Status: status, Reason: reason}
}

func steeringCheckReason(result evalResult) string {
	if !result.SteeringSent {
		return "steering was not requested for this run"
	}
	if result.Metrics.SecondsFromSteeringToNextWorker == nil {
		return "no worker was created after steering"
	}
	return fmt.Sprintf("next worker after %.2fs", *result.Metrics.SecondsFromSteeringToNextWorker)
}

func writeResult(result evalResult) error {
	if err := os.MkdirAll(filepath.Dir(result.OutputPath), 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(result.OutputPath, append(payload, '\n'), 0o644)
}

func outputPathForRun(cfg config, started time.Time, runIndex int) string {
	if strings.TrimSpace(cfg.outputPath) == "" {
		return filepath.Join("eval-results", "durable-loop-pr-producer-"+started.Format("20060102T150405Z")+".json")
	}
	if cfg.outputSet && (cfg.repeat <= 0 && cfg.maxRuns == 1 || runIndex <= 1) {
		return cfg.outputPath
	}
	ext := filepath.Ext(cfg.outputPath)
	base := strings.TrimSuffix(cfg.outputPath, ext)
	return fmt.Sprintf("%s-%s%s", base, started.Format("20060102T150405Z"), ext)
}

func printSummary(result evalResult) {
	fmt.Printf("task status before stop: %s\n", result.TaskStatusBeforeStop)
	fmt.Printf("final task status: %s\n", result.FinalTaskStatus)
	if result.FeedbackCreated {
		fmt.Printf("feedback task: %s\n", result.FeedbackTaskID)
	}
	for _, check := range result.Checks {
		fmt.Printf("%s: %s (%s)\n", check.Status, check.Name, check.Reason)
	}
}

func taskStatus(snapshot core.Snapshot, taskID string) core.TaskStatus {
	for _, task := range snapshot.Tasks {
		if task.ID == taskID {
			return task.Status
		}
	}
	return ""
}

func terminalTaskStatus(status core.TaskStatus) bool {
	return status == core.TaskSucceeded || status == core.TaskFailed || status == core.TaskCanceled
}

func payloadNestedString(payload map[string]any, parent string, key string) string {
	child, _ := payload[parent].(map[string]any)
	return payloadString(child, key)
}

func payloadString(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func decodePayload(payload json.RawMessage) map[string]any {
	var decoded map[string]any
	_ = json.Unmarshal(payload, &decoded)
	return decoded
}

func firstEventTime(events []core.Event) time.Time {
	if len(events) == 0 {
		return time.Now().UTC()
	}
	return events[0].At
}

func looksLikeRepositoryInspection(text string) bool {
	for _, marker := range []string{"gh pr", "pull request", "pr list", "jj status", "jj diff", "git status"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeTestCommand(text string) bool {
	for _, marker := range []string{"go test", "npm test", "npm run", "cargo test", "pytest"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func prBodyQuality(body string) string {
	body = strings.ToLower(body)
	hasSummary := strings.Contains(body, "summary")
	hasValidation := strings.Contains(body, "validation") || strings.Contains(body, "test")
	switch {
	case hasSummary && hasValidation:
		return "summary_and_validation"
	case hasSummary:
		return "summary_only"
	case hasValidation:
		return "validation_only"
	default:
		return "missing_summary_and_validation"
	}
}

func sampleEvents(events []core.Event, limit int) []eventSample {
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	out := make([]eventSample, 0, len(events))
	for _, event := range events {
		out = append(out, eventSample{
			ID:       event.ID,
			At:       event.At,
			Type:     event.Type,
			WorkerID: event.WorkerID,
			Summary:  eventSummary(event),
		})
	}
	return out
}

func eventSummary(event core.Event) string {
	payload := decodePayload(event.Payload)
	for _, key := range []string{"status", "kind", "summary", "text", "error"} {
		if value, _ := payload[key].(string); value != "" {
			return truncate(value, 240)
		}
	}
	return ""
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func steeringMessage(sent bool, message string) string {
	if !sent {
		return ""
	}
	return message
}
