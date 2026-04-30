package core

import (
	"encoding/json"
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
	EventTaskStatus        EventType = "task.status"
	EventTaskPlanned       EventType = "task.planned"
	EventTaskReplanned     EventType = "task.replanned"
	EventTaskSteered       EventType = "task.steered"
	EventTaskCandidate     EventType = "task.final_candidate_selected"
	EventTaskObjective     EventType = "task.objective_updated"
	EventTaskMilestone     EventType = "task.milestone_reached"
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
	EventWorkerOutput      EventType = "worker.output"
	EventWorkerCompleted   EventType = "worker.completed"
	EventWorkerApplied     EventType = "worker.changes_applied"
	EventApprovalNeeded    EventType = "approval.needed"
	EventApprovalDecided   EventType = "approval.decided"
	EventAssistantAsked    EventType = "assistant.asked"
	EventAssistantAnswered EventType = "assistant.answered"
	EventPRPublished       EventType = "pull_request.published"
	EventPRStatusChecked   EventType = "pull_request.status_checked"
	EventPRBabysitter      EventType = "pull_request.babysitter_started"
	EventPRFollowUp        EventType = "pull_request.followup_started"
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
	ID                     string          `json:"id"`
	ProjectID              string          `json:"projectId,omitempty"`
	Title                  string          `json:"title"`
	Prompt                 string          `json:"prompt"`
	Status                 TaskStatus      `json:"status"`
	ObjectiveStatus        ObjectiveStatus `json:"objectiveStatus,omitempty"`
	ObjectivePhase         string          `json:"objectivePhase,omitempty"`
	CreatedAt              time.Time       `json:"createdAt"`
	UpdatedAt              time.Time       `json:"updatedAt"`
	Metadata               json.RawMessage `json:"metadata,omitempty"`
	FinalCandidateWorkerID string          `json:"finalCandidateWorkerId,omitempty"`
	AppliedWorkerID        string          `json:"appliedWorkerId,omitempty"`
	Milestones             []TaskMilestone `json:"milestones,omitempty"`
	Artifacts              []TaskArtifact  `json:"artifacts,omitempty"`
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

type Worker struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"taskId"`
	Kind      string          `json:"kind"`
	Status    WorkerStatus    `json:"status"`
	Command   []string        `json:"command,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
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

type TargetCapacity struct {
	MaxWorkers int     `json:"maxWorkers"`
	CPUWeight  float64 `json:"cpuWeight,omitempty"`
	MemoryGB   float64 `json:"memoryGB,omitempty"`
}

type TargetState struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`
	Labels    map[string]string `json:"labels,omitempty"`
	Capacity  TargetCapacity    `json:"capacity"`
	Running   int               `json:"running"`
	Available bool              `json:"available"`
}

type Plugin struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	Protocol     string            `json:"protocol,omitempty"`
	Enabled      bool              `json:"enabled"`
	Status       string            `json:"status,omitempty"`
	Error        string            `json:"error,omitempty"`
	Command      []string          `json:"command,omitempty"`
	Endpoint     string            `json:"endpoint,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Config       map[string]string `json:"config,omitempty"`
}

type Project struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	LocalPath         string            `json:"localPath"`
	Repo              string            `json:"repo,omitempty"`
	UpstreamRepo      string            `json:"upstreamRepo,omitempty"`
	HeadRepoOwner     string            `json:"headRepoOwner,omitempty"`
	PushRemote        string            `json:"pushRemote,omitempty"`
	VCS               string            `json:"vcs,omitempty"`
	DefaultBase       string            `json:"defaultBase,omitempty"`
	WorkspaceRoot     string            `json:"workspaceRoot,omitempty"`
	TargetLabels      map[string]string `json:"targetLabels,omitempty"`
	PullRequestPolicy PullRequestPolicy `json:"pullRequestPolicy,omitempty"`
}

type PullRequestPolicy struct {
	BranchPrefix string `json:"branchPrefix,omitempty"`
	Draft        bool   `json:"draft,omitempty"`
	AllowMerge   bool   `json:"allowMerge,omitempty"`
	AutoMerge    bool   `json:"autoMerge,omitempty"`
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
	MergeStatus      string          `json:"mergeStatus,omitempty"`
	ReviewStatus     string          `json:"reviewStatus,omitempty"`
	BabysitterTaskID string          `json:"babysitterTaskId,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}

type OrchestrationGraph struct {
	TaskID    string                    `json:"taskId"`
	Status    TaskStatus                `json:"status"`
	Nodes     []OrchestrationGraphNode  `json:"nodes"`
	Edges     []OrchestrationGraphEdge  `json:"edges"`
	Summary   OrchestrationGraphSummary `json:"summary"`
	UpdatedAt time.Time                 `json:"updatedAt"`
}

type OrchestrationGraphNode struct {
	ID         string       `json:"id"`
	WorkerID   string       `json:"workerId,omitempty"`
	WorkerKind string       `json:"workerKind"`
	Status     WorkerStatus `json:"status"`
	Role       string       `json:"role,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	SpawnID    string       `json:"spawnId,omitempty"`
	TargetID   string       `json:"targetId,omitempty"`
	TargetKind string       `json:"targetKind,omitempty"`
}

type OrchestrationGraphEdge struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

type OrchestrationGraphSummary struct {
	Total    int `json:"total"`
	Running  int `json:"running"`
	Waiting  int `json:"waiting"`
	Done     int `json:"done"`
	Failed   int `json:"failed"`
	Canceled int `json:"canceled"`
}

type CreateTaskRequest struct {
	ProjectID  string          `json:"projectId,omitempty"`
	Title      string          `json:"title"`
	Prompt     string          `json:"prompt"`
	Source     string          `json:"source,omitempty"`
	ExternalID string          `json:"externalId,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
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
	WorkerID string `json:"workerId,omitempty"`
	Repo     string `json:"repo,omitempty"`
	Base     string `json:"base,omitempty"`
	Branch   string `json:"branch,omitempty"`
	Title    string `json:"title,omitempty"`
	Body     string `json:"body,omitempty"`
	Draft    bool   `json:"draft,omitempty"`
}

type SteeringRequest struct {
	Message string `json:"message"`
}

type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type Snapshot struct {
	Tasks               []Task               `json:"tasks"`
	Workers             []Worker             `json:"workers"`
	ExecutionNodes      []ExecutionNode      `json:"executionNodes"`
	Targets             []TargetState        `json:"targets,omitempty"`
	Plugins             []Plugin             `json:"plugins,omitempty"`
	Projects            []Project            `json:"projects,omitempty"`
	PullRequests        []PullRequest        `json:"pullRequests,omitempty"`
	OrchestrationGraphs []OrchestrationGraph `json:"orchestrationGraphs,omitempty"`
	Events              []Event              `json:"events"`
}

func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
