package main

import (
	"flag"
	"testing"
	"time"

	"aged/internal/flagutil"
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
		artifactMinAge:    time.Hour,
		artifactInterval:  10 * time.Minute,
		usageAware:        true,
		usageTTL:          5 * time.Minute,
		githubDriverPath:  ".config/gh.json",
		discordDriverPath: ".config/discord.json",
		webDistPath:       "web/dist",
	})

	flags := flag.NewFlagSet("aged", flag.ContinueOnError)
	var artifactCleanup bool
	var artifactDryRun bool
	githubDriver := flagutil.NewOptionalValue("")
	var discordDriver string
	var web string
	registerDaemonTestFlags(flags, &artifactCleanup, &artifactDryRun, githubDriver, &discordDriver, &web)
	if err := flags.Parse(flagutil.NormalizeOptionalValueArgs(args, "github-driver")); err != nil {
		t.Fatal(err)
	}
	if flags.NArg() != 0 {
		t.Fatalf("unparsed positional args = %v", flags.Args())
	}
	if !artifactCleanup || artifactDryRun {
		t.Fatalf("artifact cleanup = %v dryRun = %v", artifactCleanup, artifactDryRun)
	}
	if githubDriver.String() != ".config/gh.json" {
		t.Fatalf("github driver = %q, want .config/gh.json", githubDriver.String())
	}
	if discordDriver != ".config/discord.json" {
		t.Fatalf("discord driver = %q, want .config/discord.json", discordDriver)
	}
	if web != "web/dist" {
		t.Fatalf("web = %q, want web/dist", web)
	}
}

func TestLsofListenArgsUsesLinuxTCPFilter(t *testing.T) {
	got := lsofListenArgs("8787")
	want := []string{"-ti", "-sTCP:LISTEN", "-iTCP:8787"}
	if len(got) != len(want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("args = %v, want %v", got, want)
		}
	}
}

func registerDaemonTestFlags(flags *flag.FlagSet, artifactCleanup *bool, artifactDryRun *bool, githubDriver *flagutil.OptionalValue, discordDriver *string, web *string) {
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
	flags.Duration("workspace-artifact-cleanup-interval", 0, "")
	flags.Bool("usage-aware-scheduling", false, "")
	flags.Duration("usage-aware-scheduling-ttl", 0, "")
	flags.Var(githubDriver, "github-driver", "")
	flags.StringVar(discordDriver, "discord-driver", "", "")
	flags.StringVar(web, "web", "", "")
}
