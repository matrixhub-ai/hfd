// Package server assembles hfd's HTTP chain and SSH server from embedder-supplied storage, mirror, validators, policy and hooks.
package server

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet/auth"
	xetserver "github.com/wzshiming/xet/server"
	xetstorage "github.com/wzshiming/xet/storage"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	backendcas "github.com/matrixhub-ai/hfd/pkg/backend/cas"
	backendhf "github.com/matrixhub-ai/hfd/pkg/backend/hf"
	backendhttp "github.com/matrixhub-ai/hfd/pkg/backend/http"
	backendinternalapi "github.com/matrixhub-ai/hfd/pkg/backend/internalapi"
	backendlfs "github.com/matrixhub-ai/hfd/pkg/backend/lfs"
	backendssh "github.com/matrixhub-ai/hfd/pkg/backend/ssh"
	"github.com/matrixhub-ai/hfd/pkg/gc"
	"github.com/matrixhub-ai/hfd/pkg/mirror"
	"github.com/matrixhub-ai/hfd/pkg/permission"
	"github.com/matrixhub-ai/hfd/pkg/receive"
	pkgssh "github.com/matrixhub-ai/hfd/pkg/ssh"
	"github.com/matrixhub-ai/hfd/pkg/storage"
)

// Options are the pieces the server is assembled from; Storage is required, everything else is optional.
type Options struct {
	Storage        *storage.Storage
	XETStorage     xetstorage.Storage              // serves the xet CAS routes ahead of user authentication, so /v1/, /v2/, /shards, /reconstructions and /xet-bridge/ paths take precedence over hub and git routes; nil disables them
	Mirror         *mirror.Mirror                  // data plane of the lfs/hf/cas backends; nil for a plain server
	Authenticators *authenticate.Authenticators    // validators for the default authentication layer and the SSH server; nil = anonymous HTTP, unauthenticated SSH
	Authenticate   func(http.Handler) http.Handler // replaces the default authenticate.NewHandler layer when set; the xet CAS routes are answered before it
	CASAuthorizer  auth.Authorizer                 // gates the xet CAS routes; nil rejects every gated route (xorb downloads and /xet-bridge stay anonymous)
	Permission     permission.PermissionHookFunc   // nil allows every operation
	// Nil hooks are skipped.
	PreOpen     func(ctx context.Context, repoName string, write bool) error
	PreReceive  receive.PreReceiveHookFunc
	PostReceive receive.PostReceiveHookFunc
	InternalGC  *gc.Collector      // mounts the unauthenticated /internal management API when set
	GCGrace     time.Duration      // prune grace for that API; zero uses the xet default
	AccessLog   io.Writer          // combined access log; nil disables the layer
	HostURL     string             // external base URL handed to SSH clients for LFS
	Next        http.Handler       // chain tail; nil answers 404
	HFOptions   []backendhf.Option // appended after the defaults; likewise below
	LFSOptions  []backendlfs.Option
	GitOptions  []backendhttp.Option
	CASOptions  []backendcas.Option
	SSHOptions  []backendssh.Option
}

type middleware func(next http.Handler) http.Handler

func chain(tail http.Handler, middlewares ...middleware) http.Handler {
	for index := len(middlewares) - 1; index >= 0; index-- {
		tail = middlewares[index](tail)
	}
	return tail
}

func passthrough(next http.Handler) http.Handler { return next }

func requestLogging(writer io.Writer) middleware {
	if writer == nil {
		return passthrough
	}
	return func(next http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(writer, next)
	}
}

func internalAPI(o Options) middleware {
	if o.InternalGC == nil {
		return passthrough
	}
	return func(next http.Handler) http.Handler {
		return backendinternalapi.NewHandler(
			backendinternalapi.WithCollector(o.InternalGC),
			backendinternalapi.WithGCGrace(o.GCGrace),
			backendinternalapi.WithNext(next),
		)
	}
}

func authentication(o Options) middleware {
	if o.Authenticate != nil {
		return o.Authenticate
	}
	return func(next http.Handler) http.Handler {
		return authenticate.NewHandler(
			authenticate.WithNext(next),
			authenticate.WithBasicAuthValidator(o.Authenticators.BasicAuth),
			authenticate.WithTokenValidator(o.Authenticators.Token),
			authenticate.WithTokenSignValidator(o.Authenticators.TokenSign),
		)
	}
}

func gitHTTPBackend(o Options) middleware {
	return func(next http.Handler) http.Handler {
		opts := []backendhttp.Option{
			backendhttp.WithStorage(o.Storage),
			backendhttp.WithNext(next),
			backendhttp.WithPreOpenHookFunc(o.PreOpen),
			backendhttp.WithPermissionHookFunc(o.Permission),
			backendhttp.WithPreReceiveHookFunc(o.PreReceive),
			backendhttp.WithPostReceiveHookFunc(o.PostReceive),
		}
		return backendhttp.NewHandler(append(opts, o.GitOptions...)...)
	}
}

func lfsBackend(o Options) middleware {
	return func(next http.Handler) http.Handler {
		opts := []backendlfs.Option{
			backendlfs.WithStorage(o.Storage),
			backendlfs.WithNext(next),
			backendlfs.WithMirror(o.Mirror),
			backendlfs.WithPermissionHookFunc(o.Permission),
			backendlfs.WithTokenSignValidator(o.Authenticators.TokenSign),
		}
		return backendlfs.NewHandler(append(opts, o.LFSOptions...)...)
	}
}

func hfBackend(o Options) middleware {
	return func(next http.Handler) http.Handler {
		opts := []backendhf.Option{
			backendhf.WithStorage(o.Storage),
			backendhf.WithNext(next),
			backendhf.WithMirror(o.Mirror),
			backendhf.WithPreOpenHookFunc(o.PreOpen),
			backendhf.WithPermissionHookFunc(o.Permission),
			backendhf.WithPreReceiveHookFunc(o.PreReceive),
			backendhf.WithPostReceiveHookFunc(o.PostReceive),
		}
		return backendhf.NewHandler(append(opts, o.HFOptions...)...)
	}
}

func casBackend(o Options) middleware {
	return func(next http.Handler) http.Handler {
		opts := []backendcas.Option{
			backendcas.WithMirror(o.Mirror),
			backendcas.WithPermissionHookFunc(o.Permission),
			backendcas.WithNext(next),
		}
		return backendcas.NewHandler(append(opts, o.CASOptions...)...)
	}
}

func xetCASServer(o Options) middleware {
	if o.XETStorage == nil {
		return passthrough
	}
	authorizer := o.CASAuthorizer
	if authorizer == nil {
		authorizer = auth.AuthorizerFunc(func(*http.Request, auth.Grant) error { return auth.ErrUnauthenticated })
	}
	return func(next http.Handler) http.Handler {
		return xetserver.NewHandler(
			xetserver.WithStorage(o.XETStorage),
			xetserver.WithAuthorizer(authorizer),
			xetserver.WithNext(next),
		)
	}
}

// NewHTTPHandler assembles the HTTP chain: access log, internal API, xet CAS server, authentication, git, lfs, hf, cas, Next.
func NewHTTPHandler(o Options) http.Handler {
	if o.Storage == nil {
		panic("server: Options.Storage is required")
	}
	if o.Authenticators == nil {
		o.Authenticators = &authenticate.Authenticators{}
	}
	tail := o.Next
	if tail == nil {
		tail = http.NotFoundHandler()
	}
	return chain(tail,
		requestLogging(o.AccessLog),
		internalAPI(o),  // operator endpoints bypass user auth
		xetCASServer(o), // CAS credentials authorize transfers without becoming user identities
		authentication(o),
		gitHTTPBackend(o),
		lfsBackend(o),
		hfBackend(o),
		casBackend(o),
	)
}

// NewSSHServer assembles the SSH git server over the same storage, hooks, policy and validators.
func NewSSHServer(o Options, hostKey pkgssh.Signer) *backendssh.Server {
	if o.Storage == nil {
		panic("server: Options.Storage is required")
	}
	if hostKey == nil {
		panic("server: host key is required")
	}
	if o.Authenticators == nil {
		o.Authenticators = &authenticate.Authenticators{}
	}
	opts := []backendssh.Option{
		backendssh.WithStorage(o.Storage),
		backendssh.WithHostKey(hostKey),
		backendssh.WithPermissionHookFunc(o.Permission),
		backendssh.WithPreOpenHookFunc(o.PreOpen),
		backendssh.WithPreReceiveHookFunc(o.PreReceive),
		backendssh.WithPostReceiveHookFunc(o.PostReceive),
		backendssh.WithLFSURL(o.HostURL),
		backendssh.WithBasicAuthValidator(o.Authenticators.BasicAuth),
		backendssh.WithPublicKeyValidator(o.Authenticators.PublicKey),
		backendssh.WithTokenSignValidator(o.Authenticators.TokenSign),
	}
	return backendssh.NewServer(append(opts, o.SSHOptions...)...)
}
