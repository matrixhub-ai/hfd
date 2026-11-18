package mirror_test

import (
	"strings"
	"testing"

	"github.com/matrixhub-ai/hfd/pkg/mirror"
)

func TestParsePullPolicyRoundTrip(t *testing.T) {
	for text, want := range map[string]mirror.PullPolicy{"full": mirror.PullFull, "on-demand": mirror.PullOnDemand} {
		got, err := mirror.ParsePullPolicy(text)
		if err != nil || got != want {
			t.Errorf("ParsePullPolicy(%q) = %q, %v, want %q", text, got, err, want)
		}
		if got.String() != text {
			t.Errorf("ParsePullPolicy(%q).String() = %q, want the parsed text", text, got.String())
		}
	}
}

func TestParsePullPolicyRejectsOtherText(t *testing.T) {
	for _, text := range []string{"lazy", "", "Full"} {
		got, err := mirror.ParsePullPolicy(text)
		if err == nil || !strings.Contains(err.Error(), "full") || !strings.Contains(err.Error(), "on-demand") {
			t.Errorf("ParsePullPolicy(%q) = %q, %v, want an error listing full and on-demand", text, got, err)
		}
	}
}

func TestPullPolicyZeroIsUnset(t *testing.T) {
	var unset mirror.PullPolicy
	if unset == mirror.PullFull || unset == mirror.PullOnDemand {
		t.Errorf("zero PullPolicy = %q, want neither named policy", unset)
	}
}
