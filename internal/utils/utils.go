package utils

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
)

type commandOutputKey struct{}

func WithCommandOutput(ctx context.Context, output io.Writer) context.Context {
	return context.WithValue(ctx, commandOutputKey{}, output)
}

type Cmd struct {
	*exec.Cmd
	ctx context.Context
}

// Command creates an exec.Cmd with the given name and arguments, and logs the command being executed.
func Command(ctx context.Context, name string, args ...string) *Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	if name == "git" {
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	}
	return &Cmd{Cmd: cmd, ctx: ctx}
}

func (c *Cmd) setOutput() {
	output, _ := c.ctx.Value(commandOutputKey{}).(io.Writer)
	if output == nil {
		return
	}

	if c.Cmd.Stderr != nil {
		c.Cmd.Stderr = io.MultiWriter(c.Cmd.Stderr, output)
	} else {
		c.Cmd.Stderr = output
	}

	var builder strings.Builder
	if c.Cmd.Dir != "" {
		builder.WriteString("cd ")
		builder.WriteString(c.Cmd.Dir)
		builder.WriteString(" && ")
	}
	if c.Cmd.Env != nil {
		skip := 0
		osenv := os.Environ()
		if len(c.Cmd.Env) >= len(osenv) {
			for i, env := range osenv {
				if env != c.Cmd.Env[i] {
					break
				}
				skip++
			}
		}
		for _, env := range c.Cmd.Env[skip:] {
			builder.WriteString(env)
			builder.WriteString(" ")
		}
	}
	builder.WriteString(c.Cmd.Path)
	builder.WriteString(" ")
	builder.WriteString(strings.Join(c.Cmd.Args[1:], " "))
	builder.WriteString("\n")
	io.WriteString(output, builder.String())
}

func (c *Cmd) Run() error {
	c.setOutput()
	return c.Cmd.Run()
}

func (c *Cmd) Start() error {
	c.setOutput()
	return c.Cmd.Start()
}

func (c *Cmd) Output() ([]byte, error) {
	c.setOutput()
	return c.Cmd.Output()
}

func (c *Cmd) CombinedOutput() ([]byte, error) {
	c.setOutput()
	return c.Cmd.CombinedOutput()
}
