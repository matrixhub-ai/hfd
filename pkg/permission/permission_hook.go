package permission

import (
	"context"
)

// Operation represents the type of operation being performed.
type Operation uint16

const (
	operationAboutCreate Operation = 1 << iota
	operationAboutRead
	operationAboutUpdate
	operationAboutDelete
	operationAboutProxy

	operationAboutRepo

	// OperationUnknown represents an unknown or unrecognized operation.
	OperationUnknown Operation = 0
	// OperationCreateRepo represents creating a new repository.
	OperationCreateRepo = operationAboutCreate | operationAboutRepo
	// OperationDeleteRepo represents deleting an existing repository.
	OperationDeleteRepo = operationAboutDelete | operationAboutRepo
	// OperationReadRepo represents reading repository metadata (info, tree, refs, resolve).
	OperationReadRepo = operationAboutRead | operationAboutRepo
	// OperationUpdateRepo represents updating repository settings.
	OperationUpdateRepo = operationAboutUpdate | operationAboutRepo
	// OperationCreateProxyRepo represents proxying a create (mirror) from an upstream source.
	OperationCreateProxyRepo = OperationCreateRepo | operationAboutProxy
)

// String returns a human-readable name for the operation.
func (o Operation) String() string {
	switch o {
	case OperationCreateRepo:
		return "create_repo"
	case OperationDeleteRepo:
		return "delete_repo"
	case OperationReadRepo:
		return "read_repo"
	case OperationUpdateRepo:
		return "update_repo"
	case OperationCreateProxyRepo:
		return "create_proxy_repo"
	default:
		return "unknown"
	}
}

func (o Operation) IsCreate() bool {
	return o&operationAboutCreate != 0
}

func (o Operation) IsDelete() bool {
	return o&operationAboutDelete != 0
}

func (o Operation) IsUpdate() bool {
	return o&operationAboutUpdate != 0
}

func (o Operation) IsRead() bool {
	return o&operationAboutRead != 0
}

func (o Operation) IsWrite() bool {
	return o&operationAboutUpdate != 0 ||
		o&operationAboutCreate != 0 ||
		o&operationAboutDelete != 0
}

func (o Operation) IsRepoProxy() bool {
	return o&OperationCreateProxyRepo == OperationCreateProxyRepo
}

// Context holds additional context about the operation being performed.
type Context struct {
	// Ref is the branch, tag, or revision name being operated on.
	Ref string
	// DestRepo is the destination repository name (for move operations).
	DestRepo string
}

// PermissionHook is a function that checks whether an operation on a repository is allowed.
type PermissionHook func(ctx context.Context, op Operation, repoPath string, opCtx Context) error
