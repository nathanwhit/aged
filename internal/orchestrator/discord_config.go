package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"

	"aged/internal/core"
)

type DiscordDriverConfig struct {
	Enabled          bool                   `json:"enabled"`
	Token            string                 `json:"token,omitempty"`
	IntervalSeconds  int                    `json:"intervalSeconds,omitempty"`
	MessageLimit     int                    `json:"messageLimit,omitempty"`
	ProcessHistory   bool                   `json:"processHistory,omitempty"`
	AssistantProject string                 `json:"assistantProjectId,omitempty"`
	Channels         []DiscordChannelConfig `json:"channels"`
}

type DiscordChannelConfig struct {
	ID               string   `json:"id"`
	ProjectID        string   `json:"projectId,omitempty"`
	DefaultProjectID string   `json:"defaultProjectId,omitempty"`
	AllowedUserIDs   []string `json:"allowedUserIds,omitempty"`
	RequireMention   bool     `json:"requireMention,omitempty"`
	TaskPrefix       string   `json:"taskPrefix,omitempty"`
}

type DiscordUser struct {
	ID       string `json:"id"`
	Username string `json:"username,omitempty"`
	Bot      bool   `json:"bot,omitempty"`
}

type DiscordMessage struct {
	ID        string      `json:"id"`
	ChannelID string      `json:"channel_id,omitempty"`
	Content   string      `json:"content"`
	Author    DiscordUser `json:"author"`
}

type DiscordClient interface {
	Me(ctx context.Context) (DiscordUser, error)
	ListMessages(ctx context.Context, channelID string, afterID string, limit int) ([]DiscordMessage, error)
	SendMessage(ctx context.Context, channelID string, content string) error
}

type DiscordDriver struct {
	service *Service
	client  DiscordClient
	config  DiscordDriverConfig

	mu           sync.Mutex
	botID        string
	lastSeen     map[string]string
	lastProposal map[string]DiscordTaskProposal
	lastProject  map[string]string
	initialized  map[string]bool
}

type DiscordTaskProposal struct {
	ProjectID      string `json:"projectId,omitempty"`
	Title          string `json:"title,omitempty"`
	Prompt         string `json:"prompt"`
	CompletionMode string `json:"completionMode,omitempty"`
}

type DiscordAssistantDecision struct {
	Action        string
	Reply         string
	TaskID        string
	WorkerID      string
	PullRequestID string
	ProjectID     string
	TargetID      string
	PluginID      string
	Message       string
	Confirmed     bool
	Proposal      DiscordTaskProposal
	Project       core.Project
	ProjectPatch  discordProjectPatch
	Target        core.TargetConfig
	TargetPatch   discordTargetPatch
	Plugin        core.Plugin
	PluginPatch   discordPluginPatch
	PublishPR     core.PublishPullRequestRequest
	WatchPRs      core.WatchPullRequestsRequest
}

type discordProjectPatch struct {
	ID                *string                        `json:"id"`
	Name              *string                        `json:"name"`
	LocalPath         *string                        `json:"localPath"`
	Repo              *string                        `json:"repo"`
	UpstreamRepo      *string                        `json:"upstreamRepo"`
	HeadRepoOwner     *string                        `json:"headRepoOwner"`
	PushRemote        *string                        `json:"pushRemote"`
	VCS               *string                        `json:"vcs"`
	DefaultBase       *string                        `json:"defaultBase"`
	WorkspaceRoot     *string                        `json:"workspaceRoot"`
	TargetLabels      *map[string]string             `json:"targetLabels"`
	RemoteCheckouts   *map[string]string             `json:"remoteCheckouts"`
	PullRequestPolicy *discordPullRequestPolicyPatch `json:"pullRequestPolicy"`
}

type discordPullRequestPolicyPatch struct {
	BranchPrefix *string `json:"branchPrefix"`
	Draft        *bool   `json:"draft"`
	AllowMerge   *bool   `json:"allowMerge"`
	AutoMerge    *bool   `json:"autoMerge"`
}

type discordTargetPatch struct {
	ID                    *string                     `json:"id"`
	Kind                  *string                     `json:"kind"`
	Host                  *string                     `json:"host"`
	User                  *string                     `json:"user"`
	Port                  *int                        `json:"port"`
	IdentityFile          *string                     `json:"identityFile"`
	InsecureIgnoreHostKey *bool                       `json:"insecureIgnoreHostKey"`
	CheckoutRoot          *string                     `json:"checkoutRoot"`
	WorkDir               *string                     `json:"workDir"`
	WorkRoot              *string                     `json:"workRoot"`
	Labels                *map[string]string          `json:"labels"`
	Capacity              *discordTargetCapacityPatch `json:"capacity"`
}

type discordTargetCapacityPatch struct {
	MaxWorkers *int     `json:"maxWorkers"`
	CPUWeight  *float64 `json:"cpuWeight"`
	MemoryGB   *float64 `json:"memoryGB"`
}

type discordPluginPatch struct {
	ID           *string            `json:"id"`
	Name         *string            `json:"name"`
	Kind         *string            `json:"kind"`
	Protocol     *string            `json:"protocol"`
	Enabled      *bool              `json:"enabled"`
	Command      *[]string          `json:"command"`
	Endpoint     *string            `json:"endpoint"`
	Capabilities *[]string          `json:"capabilities"`
	Config       *map[string]string `json:"config"`
}

func LoadDiscordDriverConfig(value string) (DiscordDriverConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return DiscordDriverConfig{}, nil
	}
	var data []byte
	if strings.HasPrefix(value, "{") {
		data = []byte(value)
	} else {
		var err error
		data, err = os.ReadFile(value)
		if err != nil {
			return DiscordDriverConfig{}, err
		}
	}
	var config DiscordDriverConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return DiscordDriverConfig{}, err
	}
	return normalizeDiscordDriverConfig(config), nil
}

func normalizeDiscordDriverConfig(config DiscordDriverConfig) DiscordDriverConfig {
	if config.IntervalSeconds <= 0 {
		config.IntervalSeconds = 5
	}
	if config.MessageLimit <= 0 {
		config.MessageLimit = 20
	}
	if strings.TrimSpace(config.Token) == "" {
		config.Token = os.Getenv("DISCORD_BOT_TOKEN")
	}
	for index := range config.Channels {
		if strings.TrimSpace(config.Channels[index].TaskPrefix) == "" {
			config.Channels[index].TaskPrefix = "task:"
		}
		if strings.TrimSpace(config.Channels[index].DefaultProjectID) == "" {
			config.Channels[index].DefaultProjectID = config.Channels[index].ProjectID
		}
	}
	return config
}

func NewDiscordDriver(service *Service, config DiscordDriverConfig, client DiscordClient) *DiscordDriver {
	config = normalizeDiscordDriverConfig(config)
	if client == nil && strings.TrimSpace(config.Token) != "" {
		client = NewDiscordRESTClient(config.Token)
	}
	return &DiscordDriver{
		service:      service,
		client:       client,
		config:       config,
		lastSeen:     map[string]string{},
		lastProposal: map[string]DiscordTaskProposal{},
		lastProject:  map[string]string{},
		initialized:  map[string]bool{},
	}
}
