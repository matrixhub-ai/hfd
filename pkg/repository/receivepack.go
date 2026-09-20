package repository

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/go-git/gcfg/v2"
	"github.com/go-git/gcfg/v2/types"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

func (r *Repository) receivePack(ctx context.Context, output io.Writer, input io.Reader, gitProtocol string, stateless bool, hooks ReceivePackHooks) error {
	st := r.repo.Storer
	w := ioutil.NewContextWriter(ctx, output)
	if !stateless {
		if err := transport.AdvertiseRefs(ctx, st, w, transport.ReceivePackService, false, transport.ProtocolVersion(gitProtocol)); err != nil {
			return err
		}
	}
	rd := bufio.NewReader(ioutil.NewContextReader(ctx, input))
	req, _, err := readReceiveCommands(rd)
	if err != nil || len(req.Commands) == 0 {
		return err
	}
	if req.Capabilities.Supports(capability.PushOptions) {
		if err := (&packp.PushOptions{}).Decode(rd); err != nil {
			return fmt.Errorf("decoding push-options: %w", err)
		}
	}
	cfg, err := r.readReceiveConfig()
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}
	rs := &packp.ReportStatus{UnpackStatus: "ok"}
	if slices.ContainsFunc(req.Commands, func(c *packp.Command) bool { return c.Action() != packp.Delete }) {
		if err := packfile.UpdateObjectStorage(st, rd); err != nil {
			// A cancelled request is the caller's failure, not an unpack status to report.
			if ctx.Err() != nil {
				return errors.Join(err, ctx.Err())
			}
			rs.UnpackStatus = err.Error()
		}
	}
	if rs.UnpackStatus != "ok" {
		for _, cmd := range req.Commands {
			rs.CommandStatuses = append(rs.CommandStatuses, &packp.CommandStatus{ReferenceName: cmd.Name, Status: "unpacker error"})
		}
		return writeReport(w, &req.Capabilities, rs)
	}
	if hooks.PreReceive != nil {
		if denied := hooks.PreReceive(ctx, r.refUpdates(req.Commands)); denied != nil {
			return rejectReceivePack(w, rd, req, denied)
		}
	}
	var applied []*packp.Command
	for _, cmd := range req.Commands {
		status := r.updateReference(cmd, cfg)
		if status == "" {
			status = "ok"
			applied = append(applied, cmd)
		}
		rs.CommandStatuses = append(rs.CommandStatuses, &packp.CommandStatus{ReferenceName: cmd.Name, Status: status})
	}
	// Like git, the report goes out before post-receive runs.
	err = writeReport(w, &req.Capabilities, rs)
	if len(applied) > 0 && hooks.PostReceive != nil {
		hooks.PostReceive(ctx, r.refUpdates(applied))
	}
	return err
}

func (r *Repository) updateReference(cmd *packp.Command, cfg receiveConfig) string {
	st := r.repo.Storer
	if cmd.Action() != packp.Delete && st.HasEncodedObject(cmd.New) != nil {
		return "missing necessary objects"
	}
	name := cmd.Name.String()
	// Only names git accepts become paths below the repository.
	if !strings.HasPrefix(name, "refs/") || strings.Count(name, "/") < 2 || !validRefName(name) {
		return "funny refname"
	}
	if cmd.Action() == packp.Delete {
		if cmd.Name.IsBranch() && cfg.denyDeletes {
			return "deletion prohibited"
		}
		if head, herr := st.Reference(plumbing.HEAD); herr == nil && head.Target() == cmd.Name && cfg.denyDeleteCurrent {
			return "deletion of the current branch prohibited"
		}
	}
	var content string
	if cmd.Action() != packp.Delete {
		content = cmd.New.String() + "\n"
	}
	lock, err := lockRef(chroot.New(r.fs, r.repoPath), name, content)
	if err != nil {
		return "failed to update ref"
	}
	defer lock.release()
	cur, err := st.Reference(cmd.Name)
	if err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return "failed to update ref"
	}
	switch cmd.Action() {
	case packp.Create:
		if err == nil {
			return "reference already exists"
		}
	default:
		if reason := staleOld(cur, err, cmd.Old); reason != "" {
			return reason
		}
	}
	if cmd.Action() == packp.Delete {
		err = st.RemoveReference(cmd.Name)
	} else {
		err = lock.publish()
	}
	if err != nil {
		return "failed to update ref"
	}
	return ""
}

func validRefName(name string) bool {
	if strings.HasSuffix(name, ".") || strings.ContainsAny(name, " ~^:?*[\\") || strings.Contains(name, "..") || strings.Contains(name, "@{") {
		return false
	}
	for _, char := range name {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

type refLock struct {
	fs        billy.Filesystem
	name      string
	published bool
}

// Git writers coordinate through exclusive .lock files, not flock on the ref itself.
func lockRef(fs billy.Filesystem, name, content string) (*refLock, error) {
	f, err := fs.OpenFile(name+".lock", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return nil, err
	}
	l := &refLock{fs: fs, name: name}
	_, werr := f.Write([]byte(content))
	if err := errors.Join(werr, f.Close()); err != nil {
		l.release()
		return nil, err
	}
	return l, nil
}

func (l *refLock) publish() error {
	if err := l.fs.Rename(l.name+".lock", l.name); err != nil {
		return err
	}
	l.published = true
	return nil
}

// After publishing, a new lock at this path could belong to another writer.
func (l *refLock) release() {
	if !l.published {
		_ = l.fs.Remove(l.name + ".lock")
	}
}

func staleOld(cur *plumbing.Reference, lookup error, old plumbing.Hash) string {
	switch {
	case lookup != nil:
		return "reference does not exist"
	case !cur.Hash().Equal(old):
		return "incorrect old value provided"
	}
	return ""
}

type receiveConfig struct {
	denyDeletes, denyDeleteCurrent bool
}

// go-git's decoded Config loses the distinction between bare and empty values.
func (r *Repository) readReceiveConfig() (receiveConfig, error) {
	cfg := receiveConfig{denyDeleteCurrent: true}
	data, err := util.ReadFile(r.fs, filepath.Join(r.repoPath, "config"))
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	err = gcfg.ReadWithCallback(bytes.NewReader(data), func(section, subsection, key, value string, blank bool) error {
		if !strings.EqualFold(section, "receive") || subsection != "" {
			return nil
		}
		var err error
		switch strings.ToLower(key) {
		case "denydeletes":
			cfg.denyDeletes, err = gitBool(value, blank)
		case "denydeletecurrent":
			switch strings.ToLower(value) {
			case "ignore", "warn":
				cfg.denyDeleteCurrent = false
			case "refuse", "updateinstead":
				cfg.denyDeleteCurrent = true
			default:
				cfg.denyDeleteCurrent, err = gitBool(value, blank)
			}
		}
		if err != nil {
			return fmt.Errorf("receive.%s: %w", key, err)
		}
		return nil
	})
	return cfg, err
}

func gitBool(value string, blank bool) (bool, error) {
	if blank || value == "" {
		return blank, nil
	}
	if b, err := types.ParseBool(value); err == nil {
		return b, nil
	}
	number, factor := value, int64(1)
	switch value[len(value)-1] {
	case 'k', 'K':
		factor = 1 << 10
	case 'm', 'M':
		factor = 1 << 20
	case 'g', 'G':
		factor = 1 << 30
	}
	if factor != 1 {
		number = value[:len(value)-1]
	}
	unsigned := strings.TrimLeft(number, "+-")
	goSyntax := strings.Contains(number, "_") || strings.HasPrefix(strings.ToLower(unsigned), "0b") || strings.HasPrefix(strings.ToLower(unsigned), "0o")
	parsed, err := strconv.ParseInt(number, 0, 32)
	if err != nil || goSyntax || parsed < (-1<<31)/factor || parsed > (1<<31-1)/factor {
		return false, fmt.Errorf("bad boolean config value %q", value)
	}
	return parsed != 0, nil
}
