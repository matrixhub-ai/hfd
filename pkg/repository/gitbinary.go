package repository

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

// gitBinary is the git executable used for repositories on the local OS filesystem;
// empty selects go-git. Configure it before repositories are served concurrently.
var gitBinary = func() string {
	p, err := exec.LookPath("git")
	if err != nil {
		return ""
	}
	return p
}()

// gitDir returns the host directory GitBinary operates on, or "" when go-git serves r.
func (r *Repository) gitDir() string {
	if gitBinary == "" {
		return ""
	}
	return r.localDir
}

// localDir maps name on fs to a host directory; "" means fs is not a known OS-backed filesystem.
func localDir(fs billy.Basic, name string) string {
	switch fs := fs.(type) {
	case *osfs.BoundOS:
		return osHostPath(fs.Root(), name)
	case *osfs.RootOS:
		return osHostPath(fs.Root(), name)
	case *chroot.ChrootHelper:
		if crossesBoundary(name) {
			return ""
		}
		return localDir(fs.Underlying(), fs.Join(fs.Root(), name))
	}
	return ""
}

// osHostPath follows osfs: absolute names inside base are host paths, anything else is base-relative.
func osHostPath(base, name string) string {
	name = filepath.FromSlash(name)
	if filepath.IsAbs(name) {
		if rel, err := filepath.Rel(base, name); err == nil && !crossesBoundary(rel) {
			name = rel
		}
	}
	if crossesBoundary(name) {
		return ""
	}
	abs, err := filepath.Abs(filepath.Join(base, name))
	if err != nil {
		return ""
	}
	return abs
}

// crossesBoundary applies billy's chroot rule: name escapes its root once leading slashes are dropped.
func crossesBoundary(name string) bool {
	name = path.Clean(strings.TrimLeft(filepath.ToSlash(name), "/"))
	return name == ".." || strings.HasPrefix(name, "../")
}

// gitCmd builds a GitBinary command; gitProtocol is exported only when valid and config entries are appended.
func gitCmd(ctx context.Context, gitProtocol string, config []string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, gitBinary, args...)
	cmd.Env = gitEnv(gitProtocol, config)
	cmd.WaitDelay = 10 * time.Second
	return cmd
}

// gitEnv drops inherited GIT_* variables and pins git's configuration to the "key=value" entries given here.
func gitEnv(gitProtocol string, config []string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") && !strings.HasPrefix(kv, "SSH_ASKPASS=") {
			env = append(env, kv)
		}
	}
	env = append(env, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	if IsValidGitProtocol(gitProtocol) {
		env = append(env, "GIT_PROTOCOL="+gitProtocol)
	}
	config = append([]string{"safe.directory=*", "safe.bareRepository=all", "gc.auto=0", "credential.helper=", "core.askPass="}, config...)
	env = append(env, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(config)))
	for i, kv := range config {
		k, v, _ := strings.Cut(kv, "=")
		env = append(env, fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, k), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, v))
	}
	return env
}

// runGitCmd runs cmd, or waits for it when already started, folding the tail of its stderr into the returned error.
func runGitCmd(cmd *exec.Cmd) error {
	tail, ok := cmd.Stderr.(*tailBuffer)
	if !ok {
		tail = &tailBuffer{}
		cmd.Stderr = tail
	}
	var err error
	if cmd.Process != nil {
		err = cmd.Wait()
	} else {
		err = cmd.Run()
	}
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(cmd.Args[1:], " "), err, bytes.TrimSpace(tail.b))
	}
	return nil
}

const stderrTailSize = 4 << 10

// tailBuffer keeps the last stderrTailSize bytes written to it, forwarding everything to sink when set.
type tailBuffer struct {
	sink io.Writer
	b    []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	if t.sink != nil {
		_, _ = t.sink.Write(p)
	}
	t.b = append(t.b, p...)
	if n := len(t.b) - stderrTailSize; n > 0 {
		t.b = t.b[:copy(t.b, t.b[n:])]
	}
	return len(p), nil
}

// advertiseRefsGit writes the smart-HTTP service line and the service's native ref advertisement for dir.
func advertiseRefsGit(ctx context.Context, output io.Writer, dir, service, gitProtocol string) error {
	// http-backend omits the service line for any v2 request, even for receive-pack's v0 fallback.
	if transport.ProtocolVersion(gitProtocol) != protocol.V2 {
		if err := (&packp.SmartReply{Service: service}).Encode(output); err != nil {
			return err
		}
	}
	cmd := gitCmd(ctx, gitProtocol, nil, strings.TrimPrefix(service, "git-"), "--stateless-rpc", "--advertise-refs", dir)
	cmd.Stdout = output
	return runGitCmd(cmd)
}

// serveGit streams one native service request; stateless selects --stateless-rpc and stderr, when set, also receives git's stderr.
func (r *Repository) serveGit(ctx context.Context, dir, service, gitProtocol string, output io.Writer, input io.Reader, stateless bool, hooks ReceivePackHooks, stderr io.Writer) error {
	// Cancelled on return so stdin readers still waiting on the client stop once git is done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args := []string{strings.TrimPrefix(service, "git-")}
	if stateless {
		args = append(args, "--stateless-rpc")
	}
	cmd := gitCmd(ctx, gitProtocol, nil, append(args, dir)...)
	cmd.Stdout = output
	cmd.Stderr = &tailBuffer{sink: stderr}
	input = ioutil.NewContextReader(ctx, input)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if service == GitUploadPack {
		go feedStdin(stdin, input)
		err = runGitCmd(cmd)
	} else {
		err = r.receivePackGit(ctx, cmd, stdin, output, input, hooks)
	}
	if err != nil && ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

// feedStdin copies input to git's stdin; a Read blocked on a silent client ends on cancellation or once the caller closes input.
func feedStdin(stdin io.WriteCloser, input io.Reader) {
	_, _ = io.Copy(stdin, input)
	_ = stdin.Close()
}

// receivePackGit reads the client's commands itself so PreReceive can refuse them before git sees the pack; PostReceive gets only the requested refs that moved.
func (r *Repository) receivePackGit(ctx context.Context, cmd *exec.Cmd, stdin io.WriteCloser, output io.Writer, input io.Reader, hooks ReceivePackHooks) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- runGitCmd(cmd) }()
	rd := bufio.NewReader(input)
	var req *packp.UpdateRequests
	var hdr []byte
	parsed := make(chan error, 1)
	go func() {
		var err error
		req, hdr, err = readReceiveCommands(rd)
		parsed <- err
	}()
	var err error
	select {
	case err = <-parsed:
	case err = <-exited:
		// git quit before reading any command; a stateful client is still waiting for its advertisement.
		if err == nil {
			err = errors.New("git receive-pack exited before reading commands")
		}
		return err
	}
	var denied error
	if err == nil && len(req.Commands) > 0 && hooks.PreReceive != nil {
		denied = hooks.PreReceive(ctx, r.refUpdates(req.Commands))
	}
	if err != nil || denied != nil {
		_ = cmd.Process.Kill()
		<-exited
		if denied != nil {
			return rejectReceivePack(output, rd, req, denied)
		}
		return err
	}
	before := r.refSnapshot(req.Commands)
	go feedStdin(stdin, io.MultiReader(bytes.NewReader(hdr), rd))
	err = <-exited
	if len(req.Commands) == 0 {
		return err
	}
	if rerr := r.reindex(); rerr != nil {
		return errors.Join(err, rerr)
	}
	if hooks.PostReceive != nil {
		if applied := appliedCommands(req.Commands, before, r.refSnapshot(req.Commands)); len(applied) > 0 {
			hooks.PostReceive(ctx, r.refUpdates(applied))
		}
	}
	return err
}

// maxReceiveCommandsSize bounds the ref update commands held in memory while PreReceive runs.
const maxReceiveCommandsSize = 10 << 20

// readReceiveCommands reads the update commands plus the exact bytes consumed, for replay to git; a lone flush yields no commands.
func readReceiveCommands(rd *bufio.Reader) (*packp.UpdateRequests, []byte, error) {
	// Only the length is peeked: a first line longer than rd's buffer is still valid.
	prefix, err := rd.Peek(pktline.LenSize)
	if err != nil {
		return nil, nil, err
	}
	l, err := pktline.ParseLength(prefix)
	if err != nil {
		return nil, nil, err
	}
	req := &packp.UpdateRequests{}
	if l == pktline.Flush {
		_, err := rd.Discard(pktline.LenSize)
		return req, []byte("0000"), err
	}
	var hdr bytes.Buffer
	if err := req.Decode(io.TeeReader(io.LimitReader(rd, maxReceiveCommandsSize), &hdr)); err != nil {
		return nil, nil, err
	}
	return req, hdr.Bytes(), nil
}

// rejectReceivePack answers every command with reason in-protocol, draining the unread pack so the client gets to read the report.
func rejectReceivePack(output io.Writer, input io.Reader, req *packp.UpdateRequests, reason error) error {
	rs := &packp.ReportStatus{UnpackStatus: "ok"}
	for _, cmd := range req.Commands {
		rs.CommandStatuses = append(rs.CommandStatuses, &packp.CommandStatus{ReferenceName: cmd.Name, Status: reason.Error()})
	}
	err := writeReport(output, &req.Capabilities, rs)
	if err == nil {
		_, err = io.Copy(io.Discard, input)
	}
	if err != nil {
		return errors.Join(reason, err)
	}
	return reason
}

// Sideband termination is required even when report-status was not requested.
func writeReport(output io.Writer, caps *capability.List, rs *packp.ReportStatus) error {
	mux := caps.Supports(capability.Sideband64k)
	if caps.Supports(capability.ReportStatus) || caps.Supports(capability.ReportStatusV2) {
		w := output
		if mux {
			w = sideband.NewMuxer(sideband.Sideband64k, output)
		}
		if err := rs.Encode(w); err != nil {
			return err
		}
	}
	if !mux {
		return nil
	}
	return pktline.WriteFlush(output)
}

// refSnapshot returns the current hash of each command's ref; missing refs are absent.
func (r *Repository) refSnapshot(cmds []*packp.Command) map[plumbing.ReferenceName]plumbing.Hash {
	snap := make(map[plumbing.ReferenceName]plumbing.Hash, len(cmds))
	for _, cmd := range cmds {
		if ref, err := r.repo.Storer.Reference(cmd.Name); err == nil {
			snap[cmd.Name] = ref.Hash()
		}
	}
	return snap
}

// appliedCommands keeps the commands whose ref moved to the requested value between the snapshots.
func appliedCommands(cmds []*packp.Command, before, after map[plumbing.ReferenceName]plumbing.Hash) []*packp.Command {
	var applied []*packp.Command
	for _, cmd := range cmds {
		old, hadOld := before[cmd.Name]
		now, hasNow := after[cmd.Name]
		switch {
		case hadOld == hasNow && old.Equal(now):
		case cmd.Action() == packp.Delete:
			if !hasNow {
				applied = append(applied, cmd)
			}
		case hasNow && now.Equal(cmd.New):
			applied = append(applied, cmd)
		}
	}
	return applied
}

// reindex refreshes go-git's pack list after git wrote objects behind it, evicting the handle when that fails.
func (r *Repository) reindex() error {
	s, ok := r.repo.Storer.(interface{ Reindex() error })
	if !ok {
		return nil
	}
	if err := s.Reindex(); err != nil {
		lruCache.Remove(cacheKey{r.fs, r.repoPath})
		return fmt.Errorf("reindexing after git wrote objects: %w", err)
	}
	return nil
}

// mirrorRemoteConfig binds url to a one-off remote name that cannot collide with the repository's own remotes, so the URL travels only in git's environment.
func mirrorRemoteConfig(url string) (name string, config []string) {
	name = "hfd-mirror-" + rand.Text()
	return name, []string{"remote." + name + ".url=" + url}
}

// fetchGit fetches refs from url into dir with native git, streaming stderr to progress.
func (r *Repository) fetchGit(ctx context.Context, dir, url string, refs []string, progress io.Writer) error {
	var specs strings.Builder
	for _, ref := range refs {
		// Newlines split stdin refspecs; NUL truncates Git's C strings.
		if strings.ContainsAny(ref, "\n\x00") {
			return fmt.Errorf("%w: %q", plumbing.ErrInvalidReferenceName, ref)
		}
		fmt.Fprintf(&specs, "+%s:%s\n", ref, ref)
	}
	remote, config := mirrorRemoteConfig(url)
	cmd := gitCmd(ctx, "", config, "-C", dir, "fetch", "--no-tags", "--progress", "--no-write-fetch-head", "--stdin", remote)
	cmd.Stdin = strings.NewReader(specs.String())
	cmd.Stderr = &tailBuffer{sink: progress}
	err := runGitCmd(cmd)
	// A failed fetch may still have stored a pack, so the handle is refreshed either way.
	return errors.Join(err, r.reindex())
}

func (r *Repository) pushGit(ctx context.Context, dir, url string, refs []string, deletes []gitconfig.RefSpec, progress io.Writer) error {
	remote, config := mirrorRemoteConfig(url)
	args := append([]string{"-C", dir, "push", "--progress", "--", remote}, refs...)
	for _, spec := range deletes {
		args = append(args, spec.String())
	}
	cmd := gitCmd(ctx, "", append(config, "push.followTags=false"), args...)
	cmd.Stderr = &tailBuffer{sink: progress}
	return runGitCmd(cmd)
}
