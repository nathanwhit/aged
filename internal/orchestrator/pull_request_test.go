package orchestrator

import (
	"slices"
	"strings"
	"testing"
)

func TestSanitizeGitHubCLIEnvDropsCertificateOverrides(t *testing.T) {
	got := sanitizeGitHubCLIEnv([]string{
		"PATH=/opt/homebrew/bin:/usr/bin",
		"HOME=/Users/test",
		"SSL_CERT_FILE=/bad.pem",
		"SSL_CERT_DIR=/bad-certs",
		"GIT_SSL_CAINFO=/bad-git.pem",
		"REQUESTS_CA_BUNDLE=/bad-requests.pem",
		"CURL_CA_BUNDLE=/bad-curl.pem",
		"NODE_EXTRA_CA_CERTS=/bad-node.pem",
		"NIX_SSL_CERT_FILE=/bad-nix.pem",
	})
	if !slices.Contains(got, "PATH=/opt/homebrew/bin:/usr/bin") || !slices.Contains(got, "HOME=/Users/test") {
		t.Fatalf("sanitized env dropped required entries: %v", got)
	}
	for _, entry := range got {
		if strings.Contains(entry, "CERT") || strings.Contains(entry, "CA_BUNDLE") || strings.Contains(entry, "SSL_CERT") {
			t.Fatalf("sanitized env kept certificate override %q in %v", entry, got)
		}
	}
}

func TestCommandEnvOnlyCustomizesGitHubCLI(t *testing.T) {
	if env := commandEnv("jj"); env != nil {
		t.Fatalf("jj env = %v, want nil", env)
	}
	if env := commandEnv("/opt/homebrew/bin/gh"); env == nil {
		t.Fatalf("gh env = nil, want sanitized environment")
	}
}
