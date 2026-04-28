package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

type Spec struct {
	ID      string
	TaskID  string
	Kind    string
	Prompt  string
	WorkDir string
	Command []string
}

type Sink interface {
	Output(ctx context.Context, stream string, text string) error
}

type Runner interface {
	Kind() string
	BuildCommand(spec Spec) []string
	Run(ctx context.Context, spec Spec, sink Sink) error
}

type MockRunner struct{}

func (MockRunner) Kind() string {
	return "mock"
}

func (MockRunner) BuildCommand(Spec) []string {
	return nil
}

func (MockRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	if err := sink.Output(ctx, "stdout", "mock worker received task"); err != nil {
		return err
	}
	if err := sink.Output(ctx, "stdout", spec.Prompt); err != nil {
		return err
	}
	return nil
}

type CommandRunner struct {
	kind    string
	command func(Spec) []string
}

func NewCommandRunner(kind string, command func(Spec) []string) CommandRunner {
	return CommandRunner{kind: kind, command: command}
}

func (r CommandRunner) Kind() string {
	return r.kind
}

func (r CommandRunner) BuildCommand(spec Spec) []string {
	return r.command(spec)
}

func (r CommandRunner) Run(ctx context.Context, spec Spec, sink Sink) error {
	argv := r.command(spec)
	if len(argv) == 0 {
		return errors.New("empty worker command")
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if spec.WorkDir != "" {
		cmd.Dir = spec.WorkDir
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go streamLines(ctx, sink, "stdout", stdout, errCh)
	go streamLines(ctx, sink, "stderr", stderr, errCh)

	waitErr := cmd.Wait()
	for i := 0; i < 2; i++ {
		if streamErr := <-errCh; streamErr != nil {
			return streamErr
		}
	}
	if waitErr != nil {
		return fmt.Errorf("worker command failed: %w", waitErr)
	}
	return nil
}

func streamLines(ctx context.Context, sink Sink, stream string, reader io.Reader, errCh chan<- error) {
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		if err := sink.Output(ctx, stream, scanner.Text()); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- scanner.Err()
}

func DefaultRunners() map[string]Runner {
	runners := []Runner{
		MockRunner{},
		NewCommandRunner("codex", func(spec Spec) []string {
			return []string{"codex", "exec", "--json", "--cd", spec.WorkDir, spec.Prompt}
		}),
		NewCommandRunner("claude", func(spec Spec) []string {
			return []string{"claude", "-p", "--output-format", "stream-json", spec.Prompt}
		}),
		NewCommandRunner("shell", func(spec Spec) []string {
			return spec.Command
		}),
	}

	out := map[string]Runner{}
	for _, runner := range runners {
		out[runner.Kind()] = runner
	}
	return out
}
