package ssh

import (
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/authenticate"
	gossh "golang.org/x/crypto/ssh"
)

type customIdentity struct {
	authenticate.Identity
	role string
}

func TestSSHIdentity(t *testing.T) {
	id := customIdentity{Identity: authenticate.NewIdentity("deploy", "deploy@ci"), role: "writer"}
	perms := grant(id)
	if got := claim(perms); got != id {
		t.Fatalf("claim = %#v (%T); want %#v (%T)", got, got, id, id)
	}
	if got := claim(nil); got != authenticate.Anonymous {
		t.Fatalf("nil claim = %#v; want Anonymous", got)
	}
	if got := claim(&gossh.Permissions{}); got != authenticate.Anonymous {
		t.Fatalf("missing identity claim = %#v; want Anonymous", got)
	}
	if got := claim(&gossh.Permissions{ExtraData: map[any]any{identityKey{}: "not an identity"}}); got != authenticate.Anonymous {
		t.Fatalf("invalid identity claim = %#v; want Anonymous", got)
	}
	if got := claim(grant(nil)); got != authenticate.Anonymous {
		t.Fatalf("nil identity claim = %#v; want Anonymous", got)
	}
}
