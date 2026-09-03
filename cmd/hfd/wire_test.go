package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestChainOrder(t *testing.T) {
	var got []string
	tag := func(name string) middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = append(got, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	tail := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, "tail")
		w.WriteHeader(http.StatusTeapot)
	})
	serve := func(h http.Handler) int {
		got = nil
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}

	cases := []struct {
		name string
		h    http.Handler
		want []string
	}{
		{"first arg is outermost", chain(tail, tag("a"), tag("b"), tag("c")), []string{"a", "b", "c", "tail"}},
		{"no middlewares is tail", chain(tail), []string{"tail"}},
		{"passthrough is next", passthrough(tail), []string{"tail"}},
	}
	for _, tc := range cases {
		if code := serve(tc.h); code != http.StatusTeapot {
			t.Errorf("%s: status = %d, want %d", tc.name, code, http.StatusTeapot)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: order = %v, want %v", tc.name, got, tc.want)
		}
	}
}
