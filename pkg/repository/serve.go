package repository

import (
	"context"
	"io"

	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// UploadPack serves the git-upload-pack protocol for the repository over rw
// (stateful full-duplex, as used by the SSH transport). gitProtocol is the
// client's GIT_PROTOCOL request value (e.g. "version=2") and selects the
// protocol version via transport.ProtocolVersion.
func (r *Repository) UploadPack(ctx context.Context, rw io.ReadWriter, gitProtocol string) error {
	return transport.UploadPack(ctx, r.repo.Storer, io.NopCloser(rw), ioutil.WriteNopCloser(rw), &transport.UploadPackRequest{
		GitProtocol: gitProtocol,
	})
}

// ReceivePackHooks holds optional callbacks invoked while serving git-receive-pack.
type ReceivePackHooks struct {
	// PreReceive runs before any ref is updated. Returning a non-nil error
	// rejects every ref with the error message as the reason.
	PreReceive func(ctx context.Context, updates []receive.RefUpdate) error
	// PostReceive runs after refs are updated with the successfully applied updates.
	PostReceive func(ctx context.Context, updates []receive.RefUpdate)
}

// ReceivePack serves the git-receive-pack protocol for the repository over rw
// (stateful full-duplex, as used by the SSH transport). gitProtocol is the
// client's GIT_PROTOCOL request value and selects the protocol version via
// transport.ProtocolVersion.
func (r *Repository) ReceivePack(ctx context.Context, rw io.ReadWriter, gitProtocol string, hooks ReceivePackHooks) error {
	opts := &transport.ReceivePackRequest{
		GitProtocol: gitProtocol,
	}
	if hooks.PreReceive != nil {
		opts.Hooks.PreReceive = func(ctx context.Context, info *transport.PreReceiveInfo) error {
			return hooks.PreReceive(ctx, r.refUpdates(info.Commands))
		}
	}
	if hooks.PostReceive != nil {
		opts.Hooks.PostReceive = func(ctx context.Context, info *transport.PostReceiveInfo) error {
			hooks.PostReceive(ctx, r.refUpdates(info.Commands))
			return nil
		}
	}
	return transport.ReceivePack(ctx, r.repo.Storer, io.NopCloser(rw), ioutil.WriteNopCloser(rw), opts)
}

// refUpdates converts packp ref update commands to receive.RefUpdate values.
func (r *Repository) refUpdates(commands []*packp.Command) []receive.RefUpdate {
	updates := make([]receive.RefUpdate, 0, len(commands))
	for _, cmd := range commands {
		updates = append(updates, receive.NewRefUpdate(cmd.Old.String(), cmd.New.String(), cmd.Name.String(), r.repoPath))
	}
	return updates
}
