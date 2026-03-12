# hfd

HuggingFace Data (hfd) is a self-hosted Git server that implements parts of the [HuggingFace OpenAPI](https://huggingface.co/spaces/huggingface/openapi). It provides a compatible interface for hosting models, datasets, and spaces with support for Git operations, Git LFS, and HuggingFace-specific API endpoints.

## Features

- **Git Server**: Full Git protocol support over HTTP and SSH
- **Git LFS**: Large File Storage support for managing large model files
- **HuggingFace API**: Compatible with HuggingFace API endpoints for repositories, commits, branches, and more
- **Authentication**: Multiple authentication methods:
  - HTTP Basic Auth
  - Bearer tokens
  - SSH public key authentication
  - Token signing for secure access
- **Storage Options**:
  - Local filesystem storage
  - S3-compatible object storage
  - Optional S3-backed repositories with FUSE mounting
- **Mirroring/Proxy**: Can mirror and cache repositories from upstream sources like HuggingFace
- **Hooks**: Support for custom pre-receive and post-receive hooks
- **Permissions**: Configurable permission checks for repository operations

## Installation

### Using Docker

```bash
docker run -p 8080:8080 -p 2222:2222 -v ./data:/data ghcr.io/wzshiming/hfd:latest
```

### Using Docker Compose

Create a `compose.yaml` file:

```yaml
services:
  hfd:
    image: ghcr.io/wzshiming/hfd:latest
    ports:
      - "8080:8080"
      - "2222:2222"
    volumes:
      - ./data:/data
```

Then run:

```bash
docker compose up
```

### From Source

```bash
git clone https://github.com/wzshiming/hfd.git
cd hfd
go build -o hfd ./cmd/hfd
./hfd
```

## Usage

### Quick Start

Start the server with default settings:

```bash
hfd
```

The server will:
- Listen on HTTP port 8080
- Listen on SSH port 2222
- Store repositories in `./data`

### Basic Git Operations

Clone a repository over HTTP:

```bash
git clone http://localhost:8080/username/repo.git
```

Clone a repository over SSH:

```bash
git clone ssh://admin@localhost:2222/username/repo.git
```

### Using with HuggingFace CLI

Configure HuggingFace CLI to use your local server:

```bash
export HF_ENDPOINT=http://localhost:8080
huggingface-cli download username/model
```

## Configuration

hfd supports extensive configuration via command-line flags:

### Server Configuration

```bash
hfd \
  --addr :8080 \              # HTTP server address
  --ssh-addr :2222 \          # SSH server address
  --data ./data \             # Data directory
  --host-url http://localhost:8080  # External URL for the server
```

### Authentication

```bash
hfd \
  --username admin \                    # Username for basic auth
  --password mypassword \               # Password for auth
  --token mytoken \                     # Static bearer token
  --sign-key secret-key \               # Key for signing tokens
  --ssh-authorized-key ~/.ssh/authorized_keys  # SSH public keys file
  --ssh-host-key /path/to/host_key     # SSH host key (auto-generated if not provided)
```

### S3 Storage

Store repositories and LFS objects in S3:

```bash
hfd \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-access-key YOUR_ACCESS_KEY \
  --s3-secret-key YOUR_SECRET_KEY \
  --s3-bucket my-bucket \
  --s3-repositories \           # Mount S3 bucket as repository storage
  --s3-use-path-style           # Use path-style S3 URLs
```

### Mirroring/Proxy Mode

Mirror repositories from HuggingFace or another Git server:

```bash
hfd \
  --proxy https://huggingface.co \
  --mirror-ttl 1h               # Minimum time between mirror syncs
```

When proxy mode is enabled, repositories that don't exist locally will be automatically mirrored from the upstream source on first access.

## Examples

### Local Development Server

```bash
hfd --addr :8080 --ssh-addr :2222 --data ./data --password dev123
```

### Production Deployment with S3

```bash
hfd \
  --addr :8080 \
  --ssh-addr :2222 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket hfd-storage \
  --s3-access-key $AWS_ACCESS_KEY \
  --s3-secret-key $AWS_SECRET_KEY \
  --s3-repositories \
  --password $HFD_PASSWORD \
  --sign-key $HFD_SIGN_KEY
```

### HuggingFace Mirror

```bash
hfd \
  --addr :8080 \
  --proxy https://huggingface.co \
  --mirror-ttl 6h \
  --data ./mirror-cache
```

## API Compatibility

hfd implements a subset of the HuggingFace OpenAPI specification. See [hf-api-status.md](hf-api-status.md) for a detailed list of supported endpoints.

Key supported endpoints:
- Repository operations (create, move, settings)
- Branch and tag management
- Commit operations and history
- File tree browsing
- File resolution and download
- Git LFS operations
- Pre-upload checks

## Development

### Building

```bash
go build ./cmd/hfd
```

### Running Tests

```bash
go test ./...
```

### Running E2E Tests

```bash
go test ./test/e2e/...
```

## License

See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
