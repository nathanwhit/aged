package orchestrator

import "testing"

func TestLoadGitHubDriverConfigEnabledShorthand(t *testing.T) {
	config, err := LoadGitHubDriverConfig("true")
	if err != nil {
		t.Fatal(err)
	}
	if !config.Enabled {
		t.Fatalf("enabled = false, want true")
	}
	if config.IntervalSeconds != 60 || config.IssueLimit != 20 {
		t.Fatalf("config defaults = %+v", config)
	}
}

func TestLoadGitHubDriverConfigDisabledShorthand(t *testing.T) {
	config, err := LoadGitHubDriverConfig("off")
	if err != nil {
		t.Fatal(err)
	}
	if config.Enabled {
		t.Fatalf("enabled = true, want false")
	}
}
