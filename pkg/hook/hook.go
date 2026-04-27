package hook

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/matrixhub-ai/hfd/pkg/constants"
)

type HookContext struct {
	Method      string
	URL         string
	PathParams  map[string]string
	QueryParams url.Values
	Operation   string
	Headers     map[string]string
	Costume     map[string]string
}

type PermissionHook func(hCtx HookContext) (bool, error)
type PreReceiveHook func(hCtx HookContext) error
type PostReceiveHook func(hCtx HookContext) error

// HookMiddleware 是 hook 的 middleware 实现
type HookMiddleware struct {
	permissionHook  PermissionHook
	preReceiveHook  PreReceiveHook
	postReceiveHook PostReceiveHook

	routerTable map[string]string
	router      *mux.Router
}

func NewHookMiddleware(permH PermissionHook, perH PreReceiveHook, postH PostReceiveHook, tables ...map[string]string) *HookMiddleware {
	routerTable := make(map[string]string)

	for _, table := range tables {
		for op, entry := range table {
			routerTable[op] = entry
		}
	}

	// for match
	router := mux.NewRouter()
	for key := range routerTable {
		out := strings.Split(key, " ")
		if len(out) == 2 {
			router.Path(out[1]).Methods(out[0])
		}
	}

	return &HookMiddleware{
		permissionHook:  permH,
		preReceiveHook:  perH,
		postReceiveHook: postH,
		routerTable:     routerTable,
		router:          router,
	}
}

func (m *HookMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hc, err := m.buildHookContext(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if m.permissionHook != nil {
			allowed, err := m.permissionHook(hc)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !allowed {
				http.Error(w, "permission denied", http.StatusForbidden)
				return
			}
		}

		if m.preReceiveHook != nil {
			if err := m.preReceiveHook(hc); err != nil {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		costume := make(map[string]string)
		ctx := context.WithValue(r.Context(), constants.OperationChangeKey, costume)
		next.ServeHTTP(w, r.WithContext(ctx))

		if m.postReceiveHook != nil {
			if len(costume) != 0 {
				hc.Costume = costume
			}

			if err := m.postReceiveHook(hc); err != nil {
				slog.WarnContext(ctx, "post-receive hook error", "operation", hc.Operation, "error", err)
			}
		}
	})
}

func (m *HookMiddleware) buildHookContext(r *http.Request) (HookContext, error) {
	ctx := HookContext{
		Method:      r.Method,
		URL:         r.URL.String(),
		QueryParams: r.URL.Query(),
	}

	var match mux.RouteMatch
	if m.router.Match(r, &match) {
		if match.Route != nil {
			template, err := match.Route.GetPathTemplate()
			if err == nil {
				key := r.Method + " " + template
				if op, ok := m.routerTable[key]; ok {
					ctx.Operation = op
					ctx.PathParams = match.Vars
				}
			}
		}
	}

	headers := make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	ctx.Headers = headers

	return ctx, nil
}
