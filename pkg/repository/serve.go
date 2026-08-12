package repository

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"

	"github.com/matrixhub-ai/hfd/pkg/receive"
)

// Stateless serves a single stateless-RPC request for the given service, as
// used by the smart-HTTP transport. hooks apply only to git-receive-pack.
func (r *Repository) Stateless(ctx context.Context, output io.Writer, input io.Reader, service string, gitProtocol string, hooks ReceivePackHooks) error {
	w := ioutil.WriteNopCloser(output)
	in := io.NopCloser(input)
	switch service {
	case GitUploadPack:
		return transport.UploadPack(ctx, r.repo.Storer, in, w, &transport.UploadPackRequest{
			GitProtocol:  gitProtocol,
			StatelessRPC: true,
		})
	case GitReceivePack:
		return transport.ReceivePack(ctx, r.repo.Storer, in, w, &transport.ReceivePackRequest{
			GitProtocol:  gitProtocol,
			StatelessRPC: true,
			Hooks:        r.transportHooks(hooks),
		})
	default:
		return fmt.Errorf("unsupported service: %s", service)
	}
}

// AdvertiseRefs writes the smart-HTTP /info/refs advertisement for the given
// service (including the "# service=..." prefix when applicable).
func (r *Repository) AdvertiseRefs(ctx context.Context, output io.Writer, service string, gitProtocol string) error {
	w := ioutil.WriteNopCloser(output)
	input := io.NopCloser(strings.NewReader(""))
	switch service {
	case GitUploadPack:
		return transport.UploadPack(ctx, r.repo.Storer, input, w, &transport.UploadPackRequest{
			GitProtocol:   gitProtocol,
			AdvertiseRefs: true,
			StatelessRPC:  true,
		})
	case GitReceivePack:
		return transport.ReceivePack(ctx, r.repo.Storer, input, w, &transport.ReceivePackRequest{
			GitProtocol:   gitProtocol,
			AdvertiseRefs: true,
			StatelessRPC:  true,
		})
	default:
		return fmt.Errorf("unsupported service: %s", service)
	}
}

// Serve serves the given git service over rw (stateful full-duplex, as used
// by the SSH transport). gitProtocol is the client's GIT_PROTOCOL request
// value (e.g. "version=2") and selects the protocol version via
// transport.ProtocolVersion. hooks apply only to git-receive-pack.
func (r *Repository) Serve(ctx context.Context, rw io.ReadWriter, service string, gitProtocol string, hooks ReceivePackHooks) error {
	w := ioutil.WriteNopCloser(rw)
	in := io.NopCloser(rw)
	switch service {
	case GitUploadPack:
		return transport.UploadPack(ctx, r.repo.Storer, in, w, &transport.UploadPackRequest{
			GitProtocol: gitProtocol,
		})
	case GitReceivePack:
		return transport.ReceivePack(ctx, r.repo.Storer, in, w, &transport.ReceivePackRequest{
			GitProtocol: gitProtocol,
			Hooks:       r.transportHooks(hooks),
		})
	default:
		return fmt.Errorf("unsupported service: %s", service)
	}
}

// ReceivePackHooks holds optional callbacks invoked while serving git-receive-pack.
type ReceivePackHooks struct {
	// PreReceive runs before any ref is updated. Returning a non-nil error
	// rejects every ref with the error message as the reason.
	PreReceive func(ctx context.Context, updates []receive.RefUpdate) error
	// PostReceive runs after refs are updated with the successfully applied updates.
	PostReceive func(ctx context.Context, updates []receive.RefUpdate)
}

// transportHooks converts ReceivePackHooks to go-git transport hooks.
func (r *Repository) transportHooks(hooks ReceivePackHooks) transport.ReceivePackHooks {
	var out transport.ReceivePackHooks
	if hooks.PreReceive != nil {
		out.PreReceive = func(ctx context.Context, info *transport.PreReceiveInfo) error {
			return hooks.PreReceive(ctx, r.refUpdates(info.Commands))
		}
	}
	if hooks.PostReceive != nil {
		out.PostReceive = func(ctx context.Context, info *transport.PostReceiveInfo) error {
			hooks.PostReceive(ctx, r.refUpdates(info.Commands))
			return nil
		}
	}
	return out
}

// refUpdates converts packp ref update commands to receive.RefUpdate values.
func (r *Repository) refUpdates(commands []*packp.Command) []receive.RefUpdate {
	updates := make([]receive.RefUpdate, 0, len(commands))
	for _, cmd := range commands {
		updates = append(updates, receive.NewRefUpdate(cmd.Old.String(), cmd.New.String(), cmd.Name.String(), r.repoPath))
	}
	return updates
}
