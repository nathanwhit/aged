package flagutil

import (
	"flag"
	"testing"
)

func TestOptionalValueAcceptsBareFlag(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	value := NewOptionalValue("")
	flags.Var(value, "github-driver", "")

	if err := flags.Parse([]string{"-github-driver"}); err != nil {
		t.Fatal(err)
	}
	if value.String() != "true" {
		t.Fatalf("value = %q, want true", value.String())
	}
}

func TestOptionalValueAcceptsEqualsValue(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	value := NewOptionalValue("")
	flags.Var(value, "github-driver", "")

	if err := flags.Parse([]string{"-github-driver=github-driver.json"}); err != nil {
		t.Fatal(err)
	}
	if value.String() != "github-driver.json" {
		t.Fatalf("value = %q, want github-driver.json", value.String())
	}
}

func TestNormalizeOptionalValueArgsAcceptsSeparatedValue(t *testing.T) {
	args := NormalizeOptionalValueArgs([]string{"-github-driver", "github-driver.json", "-web", "web/dist"}, "github-driver")
	want := []string{"-github-driver=github-driver.json", "-web", "web/dist"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %v, want %v", args, want)
		}
	}
}
