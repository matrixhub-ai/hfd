package mirror

import (
	"context"
	"fmt"
	"io"

	"github.com/matrixhub-ai/hfd/pkg/repository"
)

// syncMirror syncs a mirror and fires pre/post-receive hooks for any ref changes.
func (m *Mirror) syncMirror(ctx context.Context, repo *repository.Repository, repoName string, sourceURL string, refs []string, progress io.Writer) error {
	remoteRefsMap, err := repository.GetRemoteRefs(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to list remote refs: %w", err)
	}

	refsFilter, err := m.filterSyncRefs(ctx, repoName, remoteRefsMap, refs)
	if err != nil {
		return err
	}
	if len(refsFilter) == 0 {
		return nil
	}

	before, err := repo.Refs()
	if err != nil {
		return fmt.Errorf("failed to get local refs: %w", err)
	}
	before = filterKeyFromMap(before, refsFilter)

	remoteMap := filterKeyFromMap(remoteRefsMap, refsFilter)
	preReceiveUpdates := repo.DiffRefs(before, remoteMap)
	if len(preReceiveUpdates) == 0 {
		return nil
	}
	if m.preReceiveHookFunc != nil {
		if ok, err := m.preReceiveHookFunc(ctx, repoName, preReceiveUpdates); err != nil {
			return fmt.Errorf("pre-receive hook error: %w", err)
		} else if !ok {
			return nil
		}
	}

	if err := repo.PullMirrorRefs(ctx, sourceURL, refsFilter, progress); err != nil {
		return fmt.Errorf("failed to sync mirror refs: %w", err)
	}

	return m.firePostReceive(ctx, repo, repoName, before, refsFilter)
}

// filterSyncRefs decides which refs to sync: an explicit list wins, then the
// configured ref filter, then all remote refs.
func (m *Mirror) filterSyncRefs(ctx context.Context, repoName string, remoteRefsMap map[string]string, refs []string) ([]string, error) {
	refsFilter := keys(remoteRefsMap)
	if len(refs) > 0 {
		return refs, nil
	}
	if m.mirrorRefFilterFunc != nil {
		filtered, err := m.mirrorRefFilterFunc(ctx, repoName, refsFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to filter mirror refs: %w", err)
		}
		return filtered, nil
	}
	return refsFilter, nil
}

// firePostReceive diffs local refs against the pre-sync snapshot and fires the
// post-receive hook for any changes.
func (m *Mirror) firePostReceive(ctx context.Context, repo *repository.Repository, repoName string, before map[string]string, refsFilter []string) error {
	if m.postReceiveHookFunc == nil {
		return nil
	}
	after, err := repo.Refs()
	if err != nil {
		return fmt.Errorf("failed to get local refs after sync: %w", err)
	}
	after = filterKeyFromMap(after, refsFilter)
	postReceiveUpdates := repo.DiffRefs(before, after)
	if len(postReceiveUpdates) > 0 {
		if err := m.postReceiveHookFunc(ctx, repoName, postReceiveUpdates); err != nil {
			return fmt.Errorf("post-receive hook error: %w", err)
		}
	}
	return nil
}

func filterKeyFromMap(m map[string]string, keys []string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string)
	for _, key := range keys {
		val, ok := m[key]
		if !ok {
			continue
		}
		result[key] = val
	}
	return result
}

func keys(m map[string]string) []string {
	var result []string
	for k := range m {
		result = append(result, k)
	}
	return result
}
