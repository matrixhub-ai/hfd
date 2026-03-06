package ssh

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"

	gossh "golang.org/x/crypto/ssh"
)

// Signer is an alias for ssh.Signer to avoid requiring callers to import golang.org/x/crypto/ssh.
type Signer = gossh.Signer

// PublicKey is an alias for ssh.PublicKey to avoid requiring callers to import golang.org/x/crypto/ssh.
type PublicKey = gossh.PublicKey

// KeyType specifies the type of SSH host key to generate.
type KeyType string

const (
	// KeyTypeEd25519 generates an Ed25519 host key.
	KeyTypeEd25519 KeyType = "ed25519"
	// KeyTypeRSA generates an RSA host key (4096-bit).
	KeyTypeRSA KeyType = "rsa"
)

// ParseAuthorizedKeys parses an OpenSSH authorized_keys file and returns
// the parsed public keys. Lines that are empty or start with '#' are skipped.
func ParseAuthorizedKeys(data []byte) ([]PublicKey, error) {
	var keys []PublicKey
	rest := data
	for len(rest) > 0 {
		var key PublicKey
		var err error
		key, _, _, rest, err = gossh.ParseAuthorizedKey(rest)
		if err != nil {
			return nil, fmt.Errorf("parsing authorized key: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// ParseHostKeyFile reads a PEM-encoded private key and returns an SSH signer.
func ParseHostKeyFile(data []byte) (Signer, error) {
	signer, err := gossh.ParsePrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("parsing host key: %w", err)
	}
	return signer, nil
}

// GenerateAndSaveHostKey generates an SSH host key of the specified type and
// saves it to path with 0600 permissions atomically, then returns a signer for the key.
func GenerateAndSaveHostKey(path string, keyType KeyType) (Signer, error) {
	var privateKey any

	switch keyType {
	case KeyTypeEd25519:
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generating Ed25519 host key: %w", err)
		}
		privateKey = priv
	case KeyTypeRSA:
		priv, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return nil, fmt.Errorf("generating RSA host key: %w", err)
		}
		privateKey = priv
	default:
		return nil, fmt.Errorf("unsupported host key type %q; supported types are %q and %q", keyType, KeyTypeEd25519, KeyTypeRSA)
	}

	derBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshaling host key: %w", err)
	}

	pemBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: derBytes,
	}

	// Write atomically: temp file in the same directory, then rename.
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".ssh_host_key_tmp_")
	if err != nil {
		return nil, fmt.Errorf("creating temp file for host key: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// Best-effort cleanup of temp file on failure.
		_ = os.Remove(tmpName)
	}()

	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("setting permissions on host key temp file: %w", err)
	}
	if _, err := tmp.Write(pem.EncodeToMemory(pemBlock)); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("writing host key to temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("syncing host key temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("closing host key temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return nil, fmt.Errorf("renaming host key temp file to %s: %w", path, err)
	}

	signer, err := gossh.NewSignerFromKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("creating signer from host key: %w", err)
	}
	return signer, nil
}
