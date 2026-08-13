package repository

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
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
		if transport.ProtocolVersion(gitProtocol) == protocol.V2 {
			return transport.UploadPack(ctx, r.repo.Storer, in, w, &transport.UploadPackRequest{
				GitProtocol:  gitProtocol,
				StatelessRPC: true,
			})
		}
		return r.serveStatelessUploadPackV0(ctx, output, input, gitProtocol)
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

// maxUploadPackRequestSize caps a buffered v0 upload-pack request body,
// matching git http-backend's http.maxRequestBuffer default. Negotiation
// bodies are small (wants plus replayed haves); the cap guards against
// oversized or decompression-bomb payloads on this unauthenticated path.
const maxUploadPackRequestSize = 10 << 20

// serveStatelessUploadPackV0 serves one stateless-RPC git-upload-pack request
// for wire protocol v0/v1.
//
// In stateless RPC every negotiation round arrives as a separate request
// (`git upload-pack --stateless-rpc` exits after each round), but go-git's
// transport.UploadPack keeps reading rounds from the same request body until
// the client sends "done" and fails with EOF on intermediate rounds. Requests
// without "done" are therefore answered here with a single ACK/NAK round,
// exactly like git http-backend; final rounds are delegated to go-git.
func (r *Repository) serveStatelessUploadPackV0(ctx context.Context, output io.Writer, input io.Reader, gitProtocol string) error {
	body, err := io.ReadAll(io.LimitReader(input, maxUploadPackRequestSize+1))
	if err != nil {
		return fmt.Errorf("reading upload-pack request: %w", err)
	}
	if len(body) > maxUploadPackRequestSize {
		return fmt.Errorf("upload-pack request larger than maximum size %d", maxUploadPackRequestSize)
	}

	if uploadPackRequestHasDone(body) {
		return transport.UploadPack(ctx, r.repo.Storer, io.NopCloser(bytes.NewReader(body)), ioutil.WriteNopCloser(output), &transport.UploadPackRequest{
			GitProtocol:  gitProtocol,
			StatelessRPC: true,
		})
	}

	return r.serveUploadPackNegotiationRound(bytes.NewReader(body), output)
}

// uploadPackRequestHasDone reports whether an upload-pack request body
// contains the final "done" pkt-line. Upload-pack requests consist solely of
// pkt-lines (wants, shallows, haves), never raw pack data.
func uploadPackRequestHasDone(body []byte) bool {
	rd := bufio.NewReader(bytes.NewReader(body))
	for {
		_, line, err := pktline.ReadLine(rd)
		if err != nil {
			return false
		}
		if strings.TrimSuffix(string(line), "\n") == "done" {
			return true
		}
	}
}

// serveUploadPackNegotiationRound answers a single v0/v1 negotiation round
// that did not conclude with "done": common objects are acknowledged
// according to the negotiated multi_ack capability and the round is closed
// with NAK. Unlike C git, the NAK is sent even when commons were found;
// fetch-pack treats NAK as the round terminator either way.
func (r *Repository) serveUploadPackNegotiationRound(body io.Reader, output io.Writer) error {
	rd := bufio.NewReader(body)

	l, _, err := pktline.PeekLine(rd)
	if err != nil {
		return fmt.Errorf("peeking upload-pack request: %w", err)
	}
	// A lone flush packet is a probe request; there is nothing to answer.
	if l == pktline.Flush {
		return nil
	}

	upreq := &packp.UploadRequest{}
	if err := upreq.Decode(rd); err != nil {
		return fmt.Errorf("decoding upload-request: %w", err)
	}
	var uphav packp.UploadHaves
	if err := uphav.Decode(rd); err != nil {
		return fmt.Errorf("decoding upload-haves: %w", err)
	}

	multiAckDetailed := upreq.Capabilities.Supports(capability.MultiACKDetailed)
	multiAck := upreq.Capabilities.Supports(capability.MultiACK)

	var acks []packp.ACK
	for _, h := range uphav.Haves {
		if r.repo.Storer.HasEncodedObject(h) != nil {
			continue
		}
		switch {
		case multiAckDetailed:
			acks = append(acks, packp.ACK{Hash: h, Status: packp.ACKCommon})
		case multiAck:
			acks = append(acks, packp.ACK{Hash: h, Status: packp.ACKContinue})
		default:
			// Single-ack mode acknowledges only the first common object.
			acks = append(acks, packp.ACK{Hash: h})
		}
		if !multiAck && !multiAckDetailed {
			break
		}
	}

	if len(acks) > 0 {
		if err := (&packp.ServerResponse{ACKs: acks}).Encode(output); err != nil {
			return fmt.Errorf("sending acks: %w", err)
		}
		if !multiAck && !multiAckDetailed {
			return nil
		}
	}
	// Close the round: the client responds with more haves or "done".
	if _, err := pktline.WriteString(output, "NAK\n"); err != nil {
		return fmt.Errorf("sending nak: %w", err)
	}
	return nil
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
