package mirror

import "fmt"

// PullPolicy decides which LFS objects a pull downloads; the zero value is unset, and any other value but PullOnDemand means PullFull.
type PullPolicy string

const (
	// PullFull prefetches every LFS object at the local ref tips in the background after the pull.
	PullFull PullPolicy = "full"
	// PullOnDemand leaves each LFS object to be fetched from the upstream when a client first reads it.
	PullOnDemand PullPolicy = "on-demand"
)

// ParsePullPolicy returns the named PullPolicy whose text form is s.
func ParsePullPolicy(s string) (PullPolicy, error) {
	switch p := PullPolicy(s); p {
	case PullFull, PullOnDemand:
		return p, nil
	}
	return "", fmt.Errorf("unknown pull policy %q, want %q or %q", s, PullFull, PullOnDemand)
}

// String returns the text form of p.
func (p PullPolicy) String() string {
	return string(p)
}
