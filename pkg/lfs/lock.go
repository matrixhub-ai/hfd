package lfs

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotOwner = errors.New("attempt to delete other user's lock")
	errNoBucket = errors.New("bucket not found")
)

// LockDB implements a metadata storage. It stores user credentials and Meta information
// for objects. The storage is handled by boltdb.
type LockDB struct {
	m   map[string][]Lock
	mut sync.RWMutex
}

// NewLock creates a new MetaStore using the boltdb database at dbFile.
func NewLock() *LockDB {
	return &LockDB{}
}

// Add write locks to the store for the repo.
func (s *LockDB) Add(repo string, l ...Lock) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.m == nil {
		s.m = make(map[string][]Lock)
	}

	s.m[repo] = append(s.m[repo], l...)
	sort.Slice(s.m[repo], func(i, j int) bool {
		return s.m[repo][i].LockedAt.Before(s.m[repo][j].LockedAt)
	})

	return nil
}

// List retrieves locks for the repo from the store
func (s *LockDB) List(repo string) ([]Lock, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.m == nil {
		s.m = make(map[string][]Lock)
	}

	return s.m[repo], nil
}

// Filtered return filtered locks for the repo
func (s *LockDB) Filtered(repo, path, cursor string, limit int) (locks []Lock, next string, err error) {
	locks, err = s.List(repo)
	if err != nil {
		return
	}

	if cursor != "" {
		lastSeen := -1
		for i, l := range locks {
			if l.Id == cursor {
				lastSeen = i
				break
			}
		}

		if lastSeen > -1 {
			locks = locks[lastSeen:]
		} else {
			err = fmt.Errorf("cursor (%s) not found", cursor)
			return
		}
	}

	if path != "" {
		var filtered []Lock
		for _, l := range locks {
			if l.Path == path {
				filtered = append(filtered, l)
			}
		}

		locks = filtered
	}

	if limit > 0 {
		size := min(limit, len(locks))
		if size+1 < len(locks) {
			next = locks[size].Id
		}
		locks = locks[:size]
	}

	return locks, next, nil
}

// Delete removes lock for the repo by id from the store
func (s *LockDB) Delete(repo, user, id string, force bool) (*Lock, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	locks, ok := s.m[repo]
	if !ok {
		return nil, errNoBucket
	}

	for i, l := range locks {
		if l.Id == id {
			if l.Owner.Name != user && !force {
				return nil, ErrNotOwner
			}
			s.m[repo] = append(locks[:i], locks[i+1:]...)
			return &l, nil
		}
	}

	return nil, fmt.Errorf("lock with id %s not found", id)
}

type User struct {
	Name string `json:"name"`
}

type Lock struct {
	Id       string    `json:"id"`
	Path     string    `json:"path"`
	Owner    User      `json:"owner"`
	LockedAt time.Time `json:"locked_at"`
}

type LockRequest struct {
	Path string `json:"path"`
}

type LockResponse struct {
	Lock    *Lock  `json:"lock"`
	Message string `json:"message,omitempty"`
}

type UnlockRequest struct {
	Force bool `json:"force"`
}

type UnlockResponse struct {
	Lock    *Lock  `json:"lock"`
	Message string `json:"message,omitempty"`
}

type LockList struct {
	Locks      []Lock `json:"locks"`
	NextCursor string `json:"next_cursor,omitempty"`
	Message    string `json:"message,omitempty"`
}

type VerifiableLockRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type VerifiableLockList struct {
	Ours       []Lock `json:"ours"`
	Theirs     []Lock `json:"theirs"`
	NextCursor string `json:"next_cursor,omitempty"`
	Message    string `json:"message,omitempty"`
}
