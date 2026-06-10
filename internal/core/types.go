package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskPlanning  TaskStatus = "planning"
	TaskRunning   TaskStatus = "running"
	TaskWaiting   TaskStatus = "waiting"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

type ObjectiveStatus string

const (
	ObjectiveActive          ObjectiveStatus = "active"
	ObjectiveWaitingExternal ObjectiveStatus = "waiting_external"
	ObjectiveWaitingUser     ObjectiveStatus = "waiting_user"
	ObjectiveSatisfied       ObjectiveStatus = "satisfied"
	ObjectiveAbandoned       ObjectiveStatus = "abandoned"
)

type WorkerStatus string

const (
	WorkerQueued    WorkerStatus = "queued"
	WorkerRunning   WorkerStatus = "running"
	WorkerWaiting   WorkerStatus = "waiting"
	WorkerSucceeded WorkerStatus = "succeeded"
	WorkerFailed    WorkerStatus = "failed"
	WorkerCanceled  WorkerStatus = "canceled"
)

type EventType string

const (
	EventTaskCreated       EventType = "task.created"
	EventTaskUpdated       EventType = "task.updated"
	EventTaskStatus        EventType = "task.status"
	EventTaskPlanned       EventType = "task.planned"
	EventTaskReplanned     EventType = "task.replanned"
	EventTaskSteered       EventType = "task.steered"
	EventTaskObjective     EventType = "task.objective_updated"
	EventTaskMilestone     EventType = "task.milestone_reached"
	EventTaskWorkPlan      EventType = "task.work_plan_updated"
	EventTaskArtifact      EventType = "task.artifact_recorded"
	EventTaskCleared       EventType = "task.cleared"
	EventTaskAction        EventType = "task.action_executed"
	EventExecutionPlanned  EventType = "execution.node_planned"
	EventExecutionStatus   EventType = "execution.node_status"
	EventApplyPolicy       EventType = "apply.policy_recommended"
	EventWorkerWorkspace   EventType = "worker.workspace_prepared"
	EventWorkerCleanup     EventType = "worker.workspace_cleaned"
	EventWorkerCreated     EventType = "worker.created"
	EventWorkerStarted     EventType = "worker.started"
	EventWorkerSteered     EventType = "worker.steering_queued"
	EventWorkerOutput      EventType = "worker.output"
	EventWorkerCompleted   EventType = "worker.completed"
	EventWorkerApplied     EventType = "worker.changes_applied"
	EventApprovalNeeded    EventType = "approval.needed"
	EventApprovalDecided   EventType = "approval.decided"
	EventAssistantAsked    EventType = "assistant.asked"
	EventAssistantAnswered EventType = "assistant.answered"
	EventPRPublished       EventType = "pull_request.published"
	EventPRUpdated         EventType = "pull_request.updated"
	EventPRStatusChecked   EventType = "pull_request.status_checked"
	EventPRBabysitter      EventType = "pull_request.babysitter_started"
	EventPRFollowUp        EventType = "pull_request.followup_started"
	EventWorkItemQueued    EventType = "work_item.queued"
	EventWorkItemStarted   EventType = "work_item.started"
	EventWorkItemCompleted EventType = "work_item.completed"
)

type Event struct {
	ID       int64           `json:"id"`
	At       time.Time       `json:"at"`
	Type     EventType       `json:"type"`
	TaskID   string          `json:"taskId,omitempty"`
	WorkerID string          `json:"workerId,omitempty"`
	Payload  json.RawMessage `json:"payload"`
}

type Task struct {
	ID              string          `json:"id"`
	ProjectID       string          `json:"projectId,omitempty"`
	WorkstreamID    string          `json:"workstreamId,omitempty"`
	Title           string          `json:"title"`
	Prompt          string          `json:"prompt"`
	Status          TaskStatus      `json:"status"`
	Error           string          `json:"error,omitempty"`
	ObjectiveStatus ObjectiveStatus `json:"objectiveStatus,omitempty"`
	ObjectivePhase  string          `json:"objectivePhase,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
	AppliedWorkerID string          `json:"appliedWorkerId,omitempty"`
	Milestones      []TaskMilestone `json:"milestones,omitempty"`
	WorkPlan        *WorkPlan       `json:"workPlan,omitempty"`
	Artifacts       []TaskArtifact  `json:"artifacts,omitempty"`
}

type TaskMilestone struct {
	Name     string          `json:"name"`
	Phase    string          `json:"phase,omitempty"`
	Summary  string          `json:"summary,omitempty"`
	At       time.Time       `json:"at"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type TaskArtifact struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Name      string          `json:"name,omitempty"`
	URL       string          `json:"url,omitempty"`
	Ref       string          `json:"ref,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type Artifact struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"taskId"`
	Kind      string          `json:"kind"`
	Name      string          `json:"name,omitempty"`
	URL       string          `json:"url,omitempty"`
	Ref       string          `json:"ref,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type MemoryEntry struct {
	ID            string          `json:"id"`
	ProjectID     string          `json:"projectId,omitempty"`
	TaskID        string          `json:"taskId,omitempty"`
	Kind          string          `json:"kind"`
	SourceEventID int64           `json:"sourceEventId,omitempty"`
	SourceEvent   string          `json:"sourceEvent,omitempty"`
	WorkerID      string          `json:"workerId,omitempty"`
	Summary       string          `json:"summary"`
	CreatedAt     time.Time       `json:"createdAt"`
	UpdatedAt     time.Time       `json:"updatedAt"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type WorkPlan struct {
	Summary     string         `json:"summary,omitempty"`
	Workstreams []WorkPlanItem `json:"workstreams,omitempty"`
	Validation  []WorkPlanItem `json:"validation,omitempty"`
	Risks       []string       `json:"risks,omitempty"`
}

type WorkPlanItem struct {
	ID        string   `json:"id"`
	Goal      string   `json:"goal"`
	Status    string   `json:"status,omitempty"`
	DoneWhen  string   `json:"doneWhen,omitempty"`
	DependsOn []string `json:"dependsOn,omitempty"`
}

func (p *WorkPlan) UnmarshalJSON(data []byte) error {
	type rawWorkPlan struct {
		Summary     string          `json:"summary,omitempty"`
		Workstreams json.RawMessage `json:"workstreams,omitempty"`
		Validation  json.RawMessage `json:"validation,omitempty"`
		Risks       json.RawMessage `json:"risks,omitempty"`
	}
	var raw rawWorkPlan
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	workstreams, err := decodeWorkPlanItems(raw.Workstreams, "workstream")
	if err != nil {
		return fmt.Errorf("decode workstreams: %w", err)
	}
	validation, err := decodeWorkPlanItems(raw.Validation, "validation")
	if err != nil {
		return fmt.Errorf("decode validation: %w", err)
	}
	risks, err := decodeWorkPlanRisks(raw.Risks)
	if err != nil {
		return fmt.Errorf("decode risks: %w", err)
	}
	p.Summary = raw.Summary
	p.Workstreams = workstreams
	p.Validation = validation
	p.Risks = risks
	return nil
}

func decodeWorkPlanItems(data json.RawMessage, prefix string) ([]WorkPlanItem, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var items []WorkPlanItem
	if err := json.Unmarshal(data, &items); err == nil {
		return normalizeWorkPlanItems(items, prefix), nil
	}
	var labels []string
	if err := json.Unmarshal(data, &labels); err == nil {
		return workPlanItemsFromLabels(labels, prefix), nil
	}
	var label string
	if err := json.Unmarshal(data, &label); err == nil {
		return workPlanItemsFromLabels([]string{label}, prefix), nil
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err == nil {
		item := workPlanItemFromObject(object, prefix, 1)
		if strings.TrimSpace(item.Goal) == "" {
			return nil, nil
		}
		return []WorkPlanItem{item}, nil
	}
	return nil, fmt.Errorf("expected object, array, string array, string, or null")
}

func normalizeWorkPlanItems(items []WorkPlanItem, prefix string) []WorkPlanItem {
	out := make([]WorkPlanItem, 0, len(items))
	for index, item := range items {
		item.Goal = strings.TrimSpace(item.Goal)
		if item.Goal == "" {
			continue
		}
		if strings.TrimSpace(item.ID) == "" {
			item.ID = fmt.Sprintf("%s-%d", prefix, index+1)
		}
		out = append(out, item)
	}
	return out
}

func workPlanItemsFromLabels(labels []string, prefix string) []WorkPlanItem {
	items := make([]WorkPlanItem, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		items = append(items, WorkPlanItem{
			ID:   fmt.Sprintf("%s-%d", prefix, len(items)+1),
			Goal: label,
		})
	}
	return items
}

func workPlanItemFromObject(object map[string]any, prefix string, index int) WorkPlanItem {
	item := WorkPlanItem{
		ID:       stringFromAny(object["id"]),
		Goal:     firstStringFromAny(object, "goal", "summary", "title", "description", "task"),
		Status:   stringFromAny(object["status"]),
		DoneWhen: firstStringFromAny(object, "doneWhen", "done_when", "acceptance", "successCriteria"),
	}
	item.DependsOn = stringSliceFromAny(object["dependsOn"])
	if len(item.DependsOn) == 0 {
		item.DependsOn = stringSliceFromAny(object["depends_on"])
	}
	if strings.TrimSpace(item.ID) == "" {
		item.ID = fmt.Sprintf("%s-%d", prefix, index)
	}
	return item
}

func decodeWorkPlanRisks(data json.RawMessage) ([]string, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	var risks []string
	if err := json.Unmarshal(data, &risks); err == nil {
		return compactNonEmptyStrings(risks), nil
	}
	var risk string
	if err := json.Unmarshal(data, &risk); err == nil {
		return compactNonEmptyStrings([]string{risk}), nil
	}
	var values []any
	if err := json.Unmarshal(data, &values); err == nil {
		risks = make([]string, 0, len(values))
		for _, value := range values {
			if risk := stringFromAny(value); strings.TrimSpace(risk) != "" {
				risks = append(risks, strings.TrimSpace(risk))
			}
		}
		return risks, nil
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err == nil {
		return compactNonEmptyStrings([]string{firstStringFromAny(object, "risk", "summary", "description", "message")}), nil
	}
	return nil, fmt.Errorf("expected object, array, string, or null")
}

func firstStringFromAny(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringFromAny(object[key]); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := stringFromAny(item); strings.TrimSpace(value) != "" {
				values = append(values, strings.TrimSpace(value))
			}
		}
		return values
	case []string:
		return compactNonEmptyStrings(typed)
	case string:
		return compactNonEmptyStrings([]string{typed})
	default:
		return nil
	}
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case map[string]any:
		return firstStringFromAny(typed, "text", "value", "summary", "description", "message", "risk", "goal", "title")
	default:
		return ""
	}
}

func compactNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

type Worker struct {
	ID          string          `json:"id"`
	TaskID      string          `json:"taskId"`
	Kind        string          `json:"kind"`
	Status      WorkerStatus    `json:"status"`
	Command     []string        `json:"command,omitempty"`
	Prompt      string          `json:"prompt,omitempty"`
	PromptPath  string          `json:"promptPath,omitempty"`
	PromptError string          `json:"promptError,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

type ExecutionNode struct {
	ID            string          `json:"id"`
	TaskID        string          `json:"taskId"`
	WorkerID      string          `json:"workerId,omitempty"`
	WorkerKind    string          `json:"workerKind"`
	Status        WorkerStatus    `json:"status"`
	PlanID        string          `json:"planId,omitempty"`
	ParentNodeID  string          `json:"parentNodeId,omitempty"`
	SpawnID       string          `json:"spawnId,omitempty"`
	Role          string          `json:"role,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	TargetID      string          `json:"targetId,omitempty"`
	TargetKind    string          `json:"targetKind,omitempty"`
	RemoteSession string          `json:"remoteSession,omitempty"`
	RemoteRunDir  string          `json:"remoteRunDir,omitempty"`
	RemoteWorkDir string          `json:"remoteWorkDir,omitempty"`
	DependsOn     []string        `json:"dependsOn,omitempty"`
	CreatedAt     time.Time       `json:"createdAt"`
	UpdatedAt     time.Time       `json:"updatedAt"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type WorkItemStatus string

const (
	WorkItemQueued    WorkItemStatus = "queued"
	WorkItemRunning   WorkItemStatus = "running"
	WorkItemSucceeded WorkItemStatus = "succeeded"
	WorkItemFailed    WorkItemStatus = "failed"
	WorkItemCanceled  WorkItemStatus = "canceled"
)

type WorkItem struct {
	ID         string          `json:"id"`
	TaskID     string          `json:"taskId"`
	Kind       string          `json:"kind"`
	Status     WorkItemStatus  `json:"status"`
	TargetKind string          `json:"targetKind,omitempty"`
	TargetID   string          `json:"targetId,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Prompt     string          `json:"prompt,omitempty"`
	WorkerID   string          `json:"workerId,omitempty"`
	LeaseOwner string          `json:"leaseOwner,omitempty"`
	LeaseUntil *time.Time      `json:"leaseUntil,omitempty"`
	Attempt    int             `json:"attempt,omitempty"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  time.Time       `json:"createdAt"`
	UpdatedAt  time.Time       `json:"updatedAt"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

type Question struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"taskId"`
	WorkerID  string          `json:"workerId,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Question  string          `json:"question"`
	Answer    string          `json:"answer,omitempty"`
	Decided   bool            `json:"decided"`
	Approved  *bool           `json:"approved,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type Session struct {
	ID                 string          `json:"id"`
	TaskID             string          `json:"taskId"`
	WorkerID           string          `json:"workerId"`
	NodeID             string          `json:"nodeId,omitempty"`
	WorkerKind         string          `json:"workerKind,omitempty"`
	Role               string          `json:"role,omitempty"`
	SpawnID            string          `json:"spawnId,omitempty"`
	Status             WorkerStatus    `json:"status"`
	TargetID           string          `json:"targetId,omitempty"`
	TargetKind         string          `json:"targetKind,omitempty"`
	RemoteSession      string          `json:"remoteSession,omitempty"`
	RemoteRunDir       string          `json:"remoteRunDir,omitempty"`
	RemoteWorkDir      string          `json:"remoteWorkDir,omitempty"`
	WorkspaceRoot      string          `json:"workspaceRoot,omitempty"`
	WorkspaceCWD       string          `json:"workspaceCwd,omitempty"`
	SourceRoot         string          `json:"sourceRoot,omitempty"`
	WorkspaceName      string          `json:"workspaceName,omitempty"`
	WorkspaceMode      string          `json:"workspaceMode,omitempty"`
	VCSType            string          `json:"vcsType,omitempty"`
	SharedRoot         string          `json:"sharedRoot,omitempty"`
	SharedArtifactsDir string          `json:"sharedArtifactsDir,omitempty"`
	SharedWorkerDir    string          `json:"sharedWorkerDir,omitempty"`
	ProviderSessionID  string          `json:"providerSessionId,omitempty"`
	CurrentAction      string          `json:"currentAction,omitempty"`
	CurrentActionLabel string          `json:"currentActionLabel,omitempty"`
	CurrentActionAt    *time.Time      `json:"currentActionAt,omitempty"`
	CurrentActionEvent int64           `json:"currentActionEvent,omitempty"`
	CreatedAt          time.Time       `json:"createdAt"`
	StartedAt          *time.Time      `json:"startedAt,omitempty"`
	UpdatedAt          time.Time       `json:"updatedAt"`
	CompletedAt        *time.Time      `json:"completedAt,omitempty"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
}

type TargetCapacity struct {
	MaxWorkers int     `json:"maxWorkers"`
	CPUWeight  float64 `json:"cpuWeight,omitempty"`
	MemoryGB   float64 `json:"memoryGB,omitempty"`
}

type TargetConfig struct {
	ID                    string            `json:"id"`
	Kind                  string            `json:"kind"`
	Host                  string            `json:"host,omitempty"`
	User                  string            `json:"user,omitempty"`
	Port                  int               `json:"port,omitempty"`
	IdentityFile          string            `json:"identityFile,omitempty"`
	InsecureIgnoreHostKey bool              `json:"insecureIgnoreHostKey,omitempty"`
	CheckoutRoot          string            `json:"checkoutRoot,omitempty"`
	WorkDir               string            `json:"workDir,omitempty"`
	WorkRoot              string            `json:"workRoot,omitempty"`
	Labels                map[string]string `json:"labels,omitempty"`
	Capacity              TargetCapacity    `json:"capacity,omitempty"`
}

type TargetState struct {
	TargetConfig
	Running   int             `json:"running"`
	Available bool            `json:"available"`
	Health    TargetHealth    `json:"health,omitempty"`
	Resources TargetResources `json:"resources,omitempty"`
}

type TargetHealth struct {
	Status      string          `json:"status,omitempty"`
	Error       string          `json:"error,omitempty"`
	CheckedAt   time.Time       `json:"checkedAt,omitempty"`
	Reachable   bool            `json:"reachable,omitempty"`
	Tmux        bool            `json:"tmux,omitempty"`
	RepoPresent bool            `json:"repoPresent,omitempty"`
	Tools       map[string]bool `json:"tools,omitempty"`
}

type TargetResources struct {
	Load1             float64 `json:"load1,omitempty"`
	CPUCount          int     `json:"cpuCount,omitempty"`
	MemoryTotalMB     int64   `json:"memoryTotalMb,omitempty"`
	MemoryAvailableMB int64   `json:"memoryAvailableMb,omitempty"`
	DiskAvailableMB   int64   `json:"diskAvailableMb,omitempty"`
	DiskUsedPercent   float64 `json:"diskUsedPercent,omitempty"`
}

type Plugin struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	Protocol     string            `json:"protocol,omitempty"`
	Enabled      bool              `json:"enabled"`
	BuiltIn      bool              `json:"builtIn,omitempty"`
	Status       string            `json:"status,omitempty"`
	Error        string            `json:"error,omitempty"`
	Command      []string          `json:"command,omitempty"`
	Endpoint     string            `json:"endpoint,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Config       map[string]string `json:"config,omitempty"`
	Driver       PluginDriverState `json:"driver,omitempty"`
}

type PluginDriverState struct {
	Managed       bool      `json:"managed,omitempty"`
	PID           int       `json:"pid,omitempty"`
	StartedAt     time.Time `json:"startedAt,omitempty"`
	LastExitAt    time.Time `json:"lastExitAt,omitempty"`
	RestartCount  int       `json:"restartCount,omitempty"`
	RestartPolicy string    `json:"restartPolicy,omitempty"`
	LogTail       []string  `json:"logTail,omitempty"`
}

type PromptSet struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Templates   map[string]string `json:"templates,omitempty"`
	BuiltIn     bool              `json:"builtIn,omitempty"`
	Default     bool              `json:"default,omitempty"`
}

type Project struct {
	ID                string              `json:"id"`
	Name              string              `json:"name"`
	LocalPath         string              `json:"localPath"`
	Repo              string              `json:"repo,omitempty"`
	UpstreamRepo      string              `json:"upstreamRepo,omitempty"`
	HeadRepoOwner     string              `json:"headRepoOwner,omitempty"`
	PushRemote        string              `json:"pushRemote,omitempty"`
	VCS               string              `json:"vcs,omitempty"`
	DefaultBase       string              `json:"defaultBase,omitempty"`
	WorkspaceRoot     string              `json:"workspaceRoot,omitempty"`
	TargetLabels      map[string]string   `json:"targetLabels,omitempty"`
	Requirements      ProjectRequirements `json:"requirements,omitempty"`
	RemoteCheckouts   map[string]string   `json:"remoteCheckouts,omitempty"`
	GitHubIssues      GitHubIssuePolicy   `json:"githubIssues,omitempty"`
	GitHubMentions    GitHubMentionPolicy `json:"githubMentions,omitempty"`
	ReviewPolicy      ReviewPolicy        `json:"reviewPolicy,omitempty"`
	PullRequestPolicy PullRequestPolicy   `json:"pullRequestPolicy,omitempty"`
}

type ProjectRequirements struct {
	MemoryMB  int64 `json:"memoryMb,omitempty"`
	StorageMB int64 `json:"storageMb,omitempty"`
}

type GitHubIssuePolicy struct {
	Enabled     bool     `json:"enabled,omitempty"`
	Labels      []string `json:"labels,omitempty"`
	IssueLimit  int      `json:"issueLimit,omitempty"`
	AutoPublish *bool    `json:"autoPublish,omitempty"`
}

type GitHubMentionPolicy struct {
	Enabled bool     `json:"enabled,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
	Limit   int      `json:"limit,omitempty"`
}

type ReviewPolicy struct {
	Enabled              bool     `json:"enabled,omitempty"`
	BeforeCompletionPR   bool     `json:"beforeCompletionPr,omitempty"`
	BeforeIntermediatePR bool     `json:"beforeIntermediatePr,omitempty"`
	BlockingSeverities   []string `json:"blockingSeverities,omitempty"`
	ReviewerKinds        []string `json:"reviewerKinds,omitempty"`
	PromptSetID          string   `json:"promptSetId,omitempty"`
	MaxAttempts          int      `json:"maxAttempts,omitempty"`
	Instructions         string   `json:"instructions,omitempty"`
}

type PullRequestPolicy struct {
	BranchPrefix        string `json:"branchPrefix,omitempty"`
	Draft               bool   `json:"draft,omitempty"`
	AllowMerge          bool   `json:"allowMerge,omitempty"`
	AutoMerge           bool   `json:"autoMerge,omitempty"`
	MergeMethod         string `json:"mergeMethod,omitempty"`
	MonitorPullRequests *bool  `json:"monitorPullRequests,omitempty"`
}

type ProjectHealth struct {
	ProjectID         string    `json:"projectId"`
	OK                bool      `json:"ok"`
	PathStatus        string    `json:"pathStatus"`
	VCSStatus         string    `json:"vcsStatus"`
	GitHubStatus      string    `json:"githubStatus,omitempty"`
	DefaultBaseStatus string    `json:"defaultBaseStatus,omitempty"`
	TargetStatus      string    `json:"targetStatus,omitempty"`
	DetectedVCS       string    `json:"detectedVcs,omitempty"`
	DetectedRepo      string    `json:"detectedRepo,omitempty"`
	DetectedBase      string    `json:"detectedBase,omitempty"`
	Errors            []string  `json:"errors,omitempty"`
	CheckedAt         time.Time `json:"checkedAt"`
}

type PullRequest struct {
	ID               string          `json:"id"`
	TaskID           string          `json:"taskId"`
	Repo             string          `json:"repo"`
	Number           int             `json:"number,omitempty"`
	URL              string          `json:"url"`
	Branch           string          `json:"branch"`
	Base             string          `json:"base"`
	Title            string          `json:"title"`
	State            string          `json:"state,omitempty"`
	Draft            bool            `json:"draft,omitempty"`
	ChecksStatus     string          `json:"checksStatus,omitempty"`
	ChecksConclusion string          `json:"checksConclusion,omitempty"`
	MergeStatus      string          `json:"mergeStatus,omitempty"`
	Mergeable        string          `json:"mergeable,omitempty"`
	ReviewStatus     string          `json:"reviewStatus,omitempty"`
	BabysitterTaskID string          `json:"babysitterTaskId,omitempty"`
	BranchOwner      string          `json:"branchOwner,omitempty"`
	BranchOwnerDir   string          `json:"branchOwnerDir,omitempty"`
	BranchHead       string          `json:"branchHead,omitempty"`
	UpdateLeaseOwner string          `json:"updateLeaseOwner,omitempty"`
	UpdateLeaseDir   string          `json:"updateLeaseDir,omitempty"`
	UpdateBaseHead   string          `json:"updateBaseHead,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}

type PullRequestFeedback struct {
	ID                string          `json:"id"`
	TaskID            string          `json:"taskId"`
	PullRequestID     string          `json:"pullRequestId"`
	EventID           int64           `json:"eventId"`
	Attempt           int             `json:"attempt,omitempty"`
	Status            string          `json:"status,omitempty"`
	Reason            string          `json:"reason,omitempty"`
	Repo              string          `json:"repo,omitempty"`
	Number            int             `json:"number,omitempty"`
	URL               string          `json:"url,omitempty"`
	Branch            string          `json:"branch,omitempty"`
	Base              string          `json:"base,omitempty"`
	State             string          `json:"state,omitempty"`
	ChecksStatus      string          `json:"checksStatus,omitempty"`
	MergeStatus       string          `json:"mergeStatus,omitempty"`
	ReviewStatus      string          `json:"reviewStatus,omitempty"`
	FeedbackSignature string          `json:"feedbackSignature,omitempty"`
	FeedbackBody      string          `json:"feedbackBody,omitempty"`
	Prompt            string          `json:"prompt,omitempty"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	HandledAt         *time.Time      `json:"handledAt,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
}

type SteeringItem struct {
	ID                string          `json:"id"`
	TaskID            string          `json:"taskId"`
	WorkerID          string          `json:"workerId,omitempty"`
	NodeID            string          `json:"nodeId,omitempty"`
	WorkerKind        string          `json:"workerKind,omitempty"`
	Role              string          `json:"role,omitempty"`
	SpawnID           string          `json:"spawnId,omitempty"`
	CandidateWorkerID string          `json:"candidateWorkerId,omitempty"`
	ReviewPhase       string          `json:"reviewPhase,omitempty"`
	TargetKind        string          `json:"targetKind,omitempty"`
	TargetID          string          `json:"targetId,omitempty"`
	Status            string          `json:"status,omitempty"`
	Reason            string          `json:"reason,omitempty"`
	Message           string          `json:"message"`
	CreatedAt         time.Time       `json:"createdAt"`
	UpdatedAt         time.Time       `json:"updatedAt"`
	AppliedAt         *time.Time      `json:"appliedAt,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
}

type TaskAssignment struct {
	ID                 string     `json:"id"`
	TaskID             string     `json:"taskId"`
	SourceKind         string     `json:"sourceKind"`
	SourceID           string     `json:"sourceId"`
	Status             string     `json:"status,omitempty"`
	Kind               string     `json:"kind,omitempty"`
	Role               string     `json:"role,omitempty"`
	WorkerID           string     `json:"workerId,omitempty"`
	WorkerKind         string     `json:"workerKind,omitempty"`
	WorkItemID         string     `json:"workItemId,omitempty"`
	NodeID             string     `json:"nodeId,omitempty"`
	SessionID          string     `json:"sessionId,omitempty"`
	TargetKind         string     `json:"targetKind,omitempty"`
	TargetID           string     `json:"targetId,omitempty"`
	ParentNodeID       string     `json:"parentNodeId,omitempty"`
	SpawnID            string     `json:"spawnId,omitempty"`
	DependsOn          []string   `json:"dependsOn,omitempty"`
	Reason             string     `json:"reason,omitempty"`
	CurrentAction      string     `json:"currentAction,omitempty"`
	CurrentActionLabel string     `json:"currentActionLabel,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
	StartedAt          *time.Time `json:"startedAt,omitempty"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	CompletedAt        *time.Time `json:"completedAt,omitempty"`
}

type TaskAssignmentsResponse struct {
	TaskID      string           `json:"taskId"`
	Assignments []TaskAssignment `json:"assignments"`
}

type CreateTaskRequest struct {
	ProjectID    string          `json:"projectId,omitempty"`
	WorkstreamID string          `json:"workstreamId,omitempty"`
	Title        string          `json:"title"`
	Prompt       string          `json:"prompt"`
	Source       string          `json:"source,omitempty"`
	ExternalID   string          `json:"externalId,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

type UpdateLoopConfigRequest struct {
	LoopIntervalSeconds *int    `json:"loopIntervalSeconds,omitempty"`
	LoopPrompt          *string `json:"loopPrompt,omitempty"`
	RequiredTargetID    *string `json:"requiredTargetID,omitempty"`
}

type AssistantRequest struct {
	ConversationID    string          `json:"conversationId,omitempty"`
	Message           string          `json:"message"`
	Context           json.RawMessage `json:"context,omitempty"`
	WorkDir           string          `json:"workDir,omitempty"`
	Provider          string          `json:"provider,omitempty"`
	ProviderSessionID string          `json:"providerSessionId,omitempty"`
}

type AssistantResponse struct {
	ConversationID    string          `json:"conversationId"`
	Message           string          `json:"message"`
	Provider          string          `json:"provider,omitempty"`
	ProviderSessionID string          `json:"providerSessionId,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
}

type PublishPullRequestRequest struct {
	WorkerID             string `json:"workerId,omitempty"`
	Repo                 string `json:"repo,omitempty"`
	Base                 string `json:"base,omitempty"`
	Branch               string `json:"branch,omitempty"`
	Title                string `json:"title,omitempty"`
	Body                 string `json:"body,omitempty"`
	FeedbackComment      string `json:"feedbackComment,omitempty"`
	CommitMessage        string `json:"commitMessage,omitempty"`
	Draft                bool   `json:"draft,omitempty"`
	ContinueAfterPublish bool   `json:"continueAfterPublish,omitempty"`
	MetadataOnly         bool   `json:"metadataOnly,omitempty"`
}

type WatchPullRequestsRequest struct {
	Repo       string `json:"repo,omitempty"`
	Number     int    `json:"number,omitempty"`
	URL        string `json:"url,omitempty"`
	State      string `json:"state,omitempty"`
	Author     string `json:"author,omitempty"`
	HeadBranch string `json:"headBranch,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type SteeringRequest struct {
	Message    string `json:"message"`
	TargetKind string `json:"targetKind,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
}

type AnswerQuestionRequest struct {
	Answer string `json:"answer"`
}

type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type Snapshot struct {
	Tasks               []Task                `json:"tasks"`
	Workers             []Worker              `json:"workers"`
	ExecutionNodes      []ExecutionNode       `json:"executionNodes"`
	WorkItems           []WorkItem            `json:"workItems,omitempty"`
	Artifacts           []Artifact            `json:"artifacts,omitempty"`
	MemoryEntries       []MemoryEntry         `json:"memoryEntries,omitempty"`
	Questions           []Question            `json:"questions,omitempty"`
	Sessions            []Session             `json:"sessions,omitempty"`
	Targets             []TargetState         `json:"targets,omitempty"`
	Plugins             []Plugin              `json:"plugins,omitempty"`
	PromptSets          []PromptSet           `json:"promptSets,omitempty"`
	Projects            []Project             `json:"projects,omitempty"`
	PullRequests        []PullRequest         `json:"pullRequests,omitempty"`
	PullRequestFeedback []PullRequestFeedback `json:"pullRequestFeedback,omitempty"`
	Steering            []SteeringItem        `json:"steering,omitempty"`
	LastEventID         int64                 `json:"lastEventId,omitempty"`
	Events              []Event               `json:"events"`
}

func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
