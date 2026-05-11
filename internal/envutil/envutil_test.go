package envutil

import (
	"testing"
	"time"
)

func TestStringDefaults(t *testing.T) {
	t.Setenv("AGED_TEST_STRING", "")
	if got := String("AGED_TEST_STRING", "fallback"); got != "fallback" {
		t.Fatalf("String empty = %q, want fallback", got)
	}
	t.Setenv("AGED_TEST_STRING", "  ")
	if got := String("AGED_TEST_STRING", "fallback"); got != "  " {
		t.Fatalf("String whitespace = %q, want original whitespace", got)
	}
	if got := TrimmedString("AGED_TEST_STRING", "fallback"); got != "fallback" {
		t.Fatalf("TrimmedString whitespace = %q, want fallback", got)
	}
}

func TestFirst(t *testing.T) {
	t.Setenv("AGED_TEST_FIRST_EMPTY", "")
	t.Setenv("AGED_TEST_FIRST_SET", "value")
	if got := First("AGED_TEST_FIRST_EMPTY", "AGED_TEST_FIRST_SET"); got != "value" {
		t.Fatalf("First = %q, want value", got)
	}
}

func TestBoolParseBoolForms(t *testing.T) {
	tests := map[string]bool{
		"1":     true,
		"t":     true,
		"T":     true,
		"TRUE":  true,
		"true":  true,
		"True":  true,
		"0":     false,
		"f":     false,
		"F":     false,
		"FALSE": false,
		"false": false,
		"False": false,
	}
	for value, want := range tests {
		t.Run(value, func(t *testing.T) {
			t.Setenv("AGED_TEST_BOOL", value)
			if got := Bool("AGED_TEST_BOOL", !want); got != want {
				t.Fatalf("Bool(%q) = %v, want %v", value, got, want)
			}
		})
	}
}

func TestBoolAliases(t *testing.T) {
	tests := map[string]bool{
		"yes": true,
		"YES": true,
		"on":  true,
		"ON":  true,
		"no":  false,
		"NO":  false,
		"off": false,
		"OFF": false,
	}
	for value, want := range tests {
		t.Run(value, func(t *testing.T) {
			t.Setenv("AGED_TEST_BOOL_ALIAS", value)
			if got := Bool("AGED_TEST_BOOL_ALIAS", !want); got != want {
				t.Fatalf("Bool(%q) = %v, want %v", value, got, want)
			}
		})
	}
}

func TestBoolInvalidFallsBack(t *testing.T) {
	t.Setenv("AGED_TEST_BOOL_INVALID", "definitely")
	if got := Bool("AGED_TEST_BOOL_INVALID", true); !got {
		t.Fatal("Bool invalid = false, want fallback true")
	}
}

func TestDuration(t *testing.T) {
	t.Setenv("AGED_TEST_DURATION", "2m")
	if got := Duration("AGED_TEST_DURATION", time.Second); got != 2*time.Minute {
		t.Fatalf("Duration = %v, want 2m", got)
	}
	t.Setenv("AGED_TEST_DURATION", "bad")
	if got := Duration("AGED_TEST_DURATION", time.Second); got != time.Second {
		t.Fatalf("Duration invalid = %v, want fallback 1s", got)
	}
}

func TestInt(t *testing.T) {
	t.Setenv("AGED_TEST_INT", "42")
	if got := Int("AGED_TEST_INT", -1); got != 42 {
		t.Fatalf("Int = %d, want 42", got)
	}
	t.Setenv("AGED_TEST_INT", "42x")
	if got := Int("AGED_TEST_INT", -1); got != -1 {
		t.Fatalf("Int invalid = %d, want fallback -1", got)
	}
}
