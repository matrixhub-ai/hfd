# Matrix-Based E2E Testing

This document describes the matrix-based e2e testing framework introduced to test all scenarios across multiple backends (HTTP, SSH) with minimal code duplication.

## Overview

The matrix testing framework allows test cases to automatically run against multiple backend protocols (HTTP and SSH) using a single test implementation. This ensures consistent test coverage across all backends while eliminating code duplication.

## Core Components

### 1. TestBackend Abstraction (`matrix_test.go`)

The `TestBackend` struct provides a unified interface for testing different backends:

```go
type TestBackend struct {
    Type         BackendType          // HTTP or SSH
    HTTPServer   *httptest.Server     // Always created for API access
    SSHListener  net.Listener         // Created only for SSH backend
    DataDir      string               // Temporary data directory
    Storage      *storage.Storage     // Shared storage
    KeyFile      string               // SSH private key file (SSH only)
}
```

Key methods:
- `GitURL(repoPath)` - Returns appropriate git URL (http:// or ssh://)
- `GitEnv()` - Returns environment variables for git commands
- `RunGitCmd(dir, args...)` - Executes git command with proper environment
- `CreateRepo(org, name)` - Creates repository via HuggingFace API
- `InitWorkDir(repoPath, localDir)` - Clones and configures git user

### 2. Backend Configuration

`BackendOptions` allows configuring backend behavior:

```go
type BackendOptions struct {
    PreReceiveHook  receive.PreReceiveHook      // Pre-receive hook
    PostReceiveHook receive.PostReceiveHook     // Post-receive hook
    PermissionHook  permission.PermissionHook   // Permission hook
    MirrorSource    repository.MirrorSourceFunc // Mirror source callback
    MirrorRefFilter repository.MirrorRefFilterFunc // Ref filter
    AuthFunc        func(username, password string) bool // Auth
    SSHAuthorized   []ssh.PublicKey             // SSH public keys
}
```

### 3. Matrix Test Runner

`RunMatrixTests` executes a test function across all specified backends:

```go
func RunMatrixTests(
    t *testing.T,
    backends []BackendType,
    opts *BackendOptions,
    testFunc func(t *testing.T, backend *TestBackend),
)
```

Example usage:
```go
func TestMatrixGitBasicOperations(t *testing.T) {
    RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil,
        func(t *testing.T, backend *TestBackend) {
            // Test implementation runs on both HTTP and SSH
            backend.CreateRepo("org", "repo")
            backend.RunGitCmd("", "clone", backend.GitURL("org/repo"), "/tmp/clone")
            // ... rest of test
        })
}
```

## Test Structure

### Matrix Git Basic Tests (`matrix_git_basic_test.go`)

Tests basic git operations across backends:

1. **TestMatrixGitBasicOperations**
   - Clone empty repository
   - Push initial commit
   - Clone with content
   - Fetch changes
   - Push additional commits
   - Pull changes
   - Verify content via HTTP (HTTP backend only)

2. **TestMatrixGitMultipleFiles**
   - Create and push multiple files
   - Verify via clone
   - Verify via HTTP GET (HTTP backend only)

3. **TestMatrixGitBranchOperations**
   - Create branch
   - Switch branch
   - Delete branch

4. **TestMatrixGitTagOperations**
   - Create lightweight tag
   - Create annotated tag
   - Delete tag

### Matrix Hook Tests (`matrix_hook_test.go`)

Tests hook functionality across backends:

1. **TestMatrixReceiveHooks**
   - Branch push triggers hook
   - Tag create triggers hook
   - Tag delete triggers hook
   - Branch create/delete triggers hook
   - Verifies correct repo name for each backend

2. **TestMatrixPreReceiveHook**
   - Branch push succeeds
   - Tag push denied by pre-receive hook

3. **TestMatrixPrePostReceiveHooks**
   - Both hooks fire with same updates
   - Verifies pre and post receive consistency

## Key Design Decisions

### 1. SSH Key Management

SSH keys are generated and authorized automatically by `RunMatrixTests`:
- Keys are created in temporary directories
- Public key is added to authorized keys
- Private key path is stored in `TestBackend.KeyFile`
- Cleanup happens automatically via `t.Cleanup()`

### 2. Backend-Specific Behavior

Tests can handle backend-specific differences:

```go
// Only test HTTP-specific features on HTTP backend
if backend.Type == BackendHTTP {
    t.Run("VerifyContentViaHTTP", func(t *testing.T) {
        resp, _ := http.Get(backend.APIURL() + "/org/repo/resolve/main/README.md")
        // ... verify
    })
}
```

Expected repo names differ by backend:
- HTTP: `"org/repo"`
- SSH: `"/org/repo.git"`

### 3. Reusing Existing Helpers

The matrix framework reuses `hookRecorder` from the original `hook_test.go` rather than duplicating it. This maintains consistency and reduces code duplication.

### 4. Handler Chain Consistency

All backends use the same handler chain order (matching `main.go`):
```
HuggingFace Handler
    ↓
LFS Handler
    ↓
HTTP/Git Handler
```

## Benefits

1. **Reduced Code Duplication**: Write test logic once, run on all backends
2. **Consistent Coverage**: Ensures HTTP and SSH behave identically
3. **Easy to Extend**: Add new backends by implementing `BackendType`
4. **Maintainable**: Changes to test logic update all backends automatically
5. **Clear Separation**: Backend-specific code is isolated to `matrix_test.go`

## Running Matrix Tests

Run all matrix tests:
```bash
go test ./test/e2e/ -run "^TestMatrix" -v
```

Run specific matrix test:
```bash
go test ./test/e2e/ -run TestMatrixGitBasicOperations -v
```

Run matrix tests for specific backend:
```bash
go test ./test/e2e/ -run "TestMatrix.*/HTTP" -v
```

## Future Enhancements

Potential areas for expansion:

1. **Auth Matrix**: Test authentication across backends with matrix
2. **LFS Matrix**: Run LFS tests on both HTTP and SSH
3. **Proxy Matrix**: Test proxy/mirror functionality across backends
4. **Performance Matrix**: Benchmark operations across backends
5. **Error Handling Matrix**: Test error scenarios consistently

## Migration Guide

To convert existing tests to matrix format:

1. Identify test that could run on multiple backends
2. Extract backend-specific setup to `BackendOptions`
3. Replace direct server setup with `RunMatrixTests`
4. Use `backend.RunGitCmd()` instead of manual git commands
5. Handle backend-specific differences with `if backend.Type == ...`

Example conversion:

**Before**:
```go
func TestHTTPGitClone(t *testing.T) {
    server, _ := setupTestServer(t)
    // ... test implementation
}

func TestSSHGitClone(t *testing.T) {
    server, listener, _ := setupSSHTestServer(t, keys)
    // ... duplicate test implementation
}
```

**After**:
```go
func TestMatrixGitClone(t *testing.T) {
    RunMatrixTests(t, []BackendType{BackendHTTP, BackendSSH}, nil,
        func(t *testing.T, backend *TestBackend) {
            // Single test implementation for both
        })
}
```

## Compatibility

The matrix testing framework is additive - it doesn't replace or modify existing tests. Both old and new tests coexist:
- Original tests: `http_git_test.go`, `ssh_test.go`, `hook_test.go`, etc.
- Matrix tests: `matrix_git_basic_test.go`, `matrix_hook_test.go`

This allows gradual migration while maintaining backward compatibility.
