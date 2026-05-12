package main

import (
	"flag"
	"testing"
	"time"
)

func TestBuildDaemonArgsKeepsFlagsAfterBooleanOptionsParseable(t *testing.T) {
	args := buildDaemonArgs(daemonConfig{
		daemonAddr:        "127.0.0.1:8787",
		dbPath:            "aged.db",
		workerKind:        "codex",
		assistantMode:     "auto",
		assistantReason:   "medium",
		brainMode:         "codex",
		workDir:           ".",
		projectsPath:      "",
		pluginsPath:       "",
		workspaceVCS:      "auto",
		workspaceMode:     "isolated",
		workspaceRoot:     "",
		workspaceCleanup:  "retain",
		artifactCleanup:   true,
		artifactDryRun:    false,
		artifactMinAge:    24 * time.Hour,
		githubDriverPath:  ".config/gh.json",
		discordDriverPath: ".config/discord.json",
		webDistPath:       "web/dist",
	})

	flags := flag.NewFlagSet("aged", flag.ContinueOnError)
	var artifactCleanup bool
	var artifactDryRun bool
	var githubDriver string
	var discordDriver string
	var web string
	registerDaemonTestFlags(flags, &artifactCleanup, &artifactDryRun, &githubDriver, &discordDriver, &web)
	if err := flags.Parse(args); err != nil {
		t.Fatal(err)
	}
	if flags.NArg() != 0 {
		t.Fatalf("unparsed positional args = %v", flags.Args())
	}
	if !artifactCleanup || artifactDryRun {
		t.Fatalf("artifact cleanup = %v dryRun = %v", artifactCleanup, artifactDryRun)
	}
	if githubDriver != ".config/gh.json" {
		t.Fatalf("github driver = %q, want .config/gh.json", githubDriver)
	}
	if discordDriver != ".config/discord.json" {
		t.Fatalf("discord driver = %q, want .config/discord.json", discordDriver)
	}
	if web != "web/dist" {
		t.Fatalf("web = %q, want web/dist", web)
	}
}

func registerDaemonTestFlags(flags *flag.FlagSet, artifactCleanup *bool, artifactDryRun *bool, githubDriver *string, discordDriver *string, web *string) {
	flags.String("addr", "", "")
	flags.String("db", "", "")
	flags.String("worker", "", "")
	flags.String("assistant", "", "")
	flags.String("assistant-reasoning", "", "")
	flags.String("brain", "", "")
	flags.String("workdir", "", "")
	flags.String("projects", "", "")
	flags.String("plugins", "", "")
	flags.String("workspace-vcs", "", "")
	flags.String("workspace-mode", "", "")
	flags.String("workspace-root", "", "")
	flags.String("workspace-cleanup", "", "")
	flags.BoolVar(artifactCleanup, "workspace-artifact-cleanup", false, "")
	flags.BoolVar(artifactDryRun, "workspace-artifact-cleanup-dry-run", false, "")
	flags.Duration("workspace-artifact-cleanup-min-age", 0, "")
	flags.StringVar(githubDriver, "github-driver", "", "")
	flags.StringVar(discordDriver, "discord-driver", "", "")
	flags.StringVar(web, "web", "", "")
}
