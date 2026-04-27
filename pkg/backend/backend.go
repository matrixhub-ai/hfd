package backend

import "net/http"

type RouterEntry struct {
	Operation string
	Method    string
	Handler   http.HandlerFunc
}

type RouterMap map[string]RouterEntry
