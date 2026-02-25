package repository

import (
	"testing"
)

func TestGitAttributesNil(t *testing.T) {
	var attrs *GitAttributes
	if attrs.IsLFS("model.bin") {
		t.Error("Expected nil GitAttributes to return false")
	}
}
