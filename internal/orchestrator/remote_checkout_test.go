package orchestrator

import (
	"testing"

	"aged/internal/core"
)

func TestResolveRemoteCheckoutHonorsProjectTargetOverride(t *testing.T) {
	checkout, err := resolveRemoteCheckout(core.Project{
		ID:              "node",
		RemoteCheckouts: map[string]string{"vm-1": "/opt/checkouts/node-custom"},
	}, TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", CheckoutRoot: "/srv/aged/checkouts"})
	if err != nil {
		t.Fatal(err)
	}
	if checkout != "/opt/checkouts/node-custom" {
		t.Fatalf("checkout = %q, want override", checkout)
	}
}

func TestResolveRemoteCheckoutDerivesProjectScopedPathUnderTargetRoot(t *testing.T) {
	checkout, err := resolveRemoteCheckout(core.Project{ID: "node"}, TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", CheckoutRoot: "/srv/aged/checkouts"})
	if err != nil {
		t.Fatal(err)
	}
	if checkout != "/srv/aged/checkouts/node" {
		t.Fatalf("checkout = %q, want project-scoped path", checkout)
	}
}

func TestResolveRemoteCheckoutSeparatesProjectsOnSharedTarget(t *testing.T) {
	target := TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", CheckoutRoot: "/srv/aged/checkouts"}
	nodeCheckout, err := resolveRemoteCheckout(core.Project{ID: "node"}, target)
	if err != nil {
		t.Fatal(err)
	}
	denoCheckout, err := resolveRemoteCheckout(core.Project{ID: "deno"}, target)
	if err != nil {
		t.Fatal(err)
	}
	if nodeCheckout == denoCheckout {
		t.Fatalf("shared target checkouts collided: %q", nodeCheckout)
	}
	if nodeCheckout != "/srv/aged/checkouts/node" || denoCheckout != "/srv/aged/checkouts/deno" {
		t.Fatalf("checkouts = %q, %q", nodeCheckout, denoCheckout)
	}
}

func TestResolveRemoteCheckoutTreatsTargetWorkDirAsCompatibilityCheckoutRoot(t *testing.T) {
	checkout, err := resolveRemoteCheckout(core.Project{ID: "node"}, TargetConfig{ID: "vm-1", Kind: TargetKindSSH, Host: "vm", WorkDir: "/legacy/repo-root"})
	if err != nil {
		t.Fatal(err)
	}
	if checkout != "/legacy/repo-root/node" {
		t.Fatalf("checkout = %q, want legacy workDir root fallback", checkout)
	}
}
