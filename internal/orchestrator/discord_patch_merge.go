package orchestrator

import (
	"strings"

	"aged/internal/core"
)

func mergeDiscordProjectPatch(current core.Project, _ core.Project, patch discordProjectPatch) core.Project {
	updated := current
	if patch.Name != nil {
		updated.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.LocalPath != nil {
		updated.LocalPath = strings.TrimSpace(*patch.LocalPath)
	}
	if patch.Repo != nil {
		updated.Repo = strings.TrimSpace(*patch.Repo)
	}
	if patch.UpstreamRepo != nil {
		updated.UpstreamRepo = strings.TrimSpace(*patch.UpstreamRepo)
	}
	if patch.HeadRepoOwner != nil {
		updated.HeadRepoOwner = strings.TrimSpace(*patch.HeadRepoOwner)
	}
	if patch.PushRemote != nil {
		updated.PushRemote = strings.TrimSpace(*patch.PushRemote)
	}
	if patch.VCS != nil {
		updated.VCS = strings.TrimSpace(*patch.VCS)
	}
	if patch.DefaultBase != nil {
		updated.DefaultBase = strings.TrimSpace(*patch.DefaultBase)
	}
	if patch.WorkspaceRoot != nil {
		updated.WorkspaceRoot = strings.TrimSpace(*patch.WorkspaceRoot)
	}
	if patch.TargetLabels != nil {
		updated.TargetLabels = *patch.TargetLabels
	}
	if patch.RemoteCheckouts != nil {
		updated.RemoteCheckouts = *patch.RemoteCheckouts
	}
	if patch.PullRequestPolicy != nil {
		if patch.PullRequestPolicy.BranchPrefix != nil {
			updated.PullRequestPolicy.BranchPrefix = strings.TrimSpace(*patch.PullRequestPolicy.BranchPrefix)
		}
		if patch.PullRequestPolicy.Draft != nil {
			updated.PullRequestPolicy.Draft = *patch.PullRequestPolicy.Draft
		}
		if patch.PullRequestPolicy.AllowMerge != nil {
			updated.PullRequestPolicy.AllowMerge = *patch.PullRequestPolicy.AllowMerge
		}
		if patch.PullRequestPolicy.AutoMerge != nil {
			updated.PullRequestPolicy.AutoMerge = *patch.PullRequestPolicy.AutoMerge
		}
	}
	updated.ID = current.ID
	return updated
}

func mergeDiscordTargetPatch(current core.TargetConfig, _ core.TargetConfig, patch discordTargetPatch) core.TargetConfig {
	updated := current
	if patch.Kind != nil {
		updated.Kind = strings.TrimSpace(*patch.Kind)
	}
	if patch.Host != nil {
		updated.Host = strings.TrimSpace(*patch.Host)
	}
	if patch.User != nil {
		updated.User = strings.TrimSpace(*patch.User)
	}
	if patch.Port != nil {
		updated.Port = *patch.Port
	}
	if patch.IdentityFile != nil {
		updated.IdentityFile = strings.TrimSpace(*patch.IdentityFile)
	}
	if patch.InsecureIgnoreHostKey != nil {
		updated.InsecureIgnoreHostKey = *patch.InsecureIgnoreHostKey
	}
	if patch.CheckoutRoot != nil {
		updated.CheckoutRoot = strings.TrimSpace(*patch.CheckoutRoot)
	}
	if patch.WorkDir != nil {
		updated.WorkDir = strings.TrimSpace(*patch.WorkDir)
	}
	if patch.WorkRoot != nil {
		updated.WorkRoot = strings.TrimSpace(*patch.WorkRoot)
	}
	if patch.Labels != nil {
		updated.Labels = *patch.Labels
	}
	if patch.Capacity != nil {
		if patch.Capacity.MaxWorkers != nil {
			updated.Capacity.MaxWorkers = *patch.Capacity.MaxWorkers
		}
		if patch.Capacity.CPUWeight != nil {
			updated.Capacity.CPUWeight = *patch.Capacity.CPUWeight
		}
		if patch.Capacity.MemoryGB != nil {
			updated.Capacity.MemoryGB = *patch.Capacity.MemoryGB
		}
	}
	updated.ID = current.ID
	updated = NormalizeSSHTargetCheckoutAliasesAfterPatch(updated, patch.CheckoutRoot != nil, patch.WorkDir != nil)
	return updated
}

func mergeDiscordPluginPatch(current core.Plugin, _ core.Plugin, patch discordPluginPatch) core.Plugin {
	updated := current
	if patch.Name != nil {
		updated.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.Kind != nil {
		updated.Kind = strings.TrimSpace(*patch.Kind)
	}
	if patch.Protocol != nil {
		updated.Protocol = strings.TrimSpace(*patch.Protocol)
	}
	if patch.Enabled != nil {
		updated.Enabled = *patch.Enabled
	}
	if patch.Command != nil {
		updated.Command = *patch.Command
	}
	if patch.Endpoint != nil {
		updated.Endpoint = strings.TrimSpace(*patch.Endpoint)
	}
	if patch.Capabilities != nil {
		updated.Capabilities = *patch.Capabilities
	}
	if patch.Config != nil {
		updated.Config = *patch.Config
	}
	updated.Status = ""
	updated.Error = ""
	updated.ID = current.ID
	updated.BuiltIn = current.BuiltIn
	return updated
}
