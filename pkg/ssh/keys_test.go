package ssh_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	gossh "golang.org/x/crypto/ssh"

	pkgssh "github.com/wzshiming/hfd/pkg/ssh"
)

func TestParseAuthorizedKeys(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer, err := gossh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}

	pubKey := signer.PublicKey()
	authorizedKey := gossh.MarshalAuthorizedKey(pubKey)

	t.Run("SingleKey", func(t *testing.T) {
		keys, err := pkgssh.ParseAuthorizedKeys(authorizedKey)
		if err != nil {
			t.Fatalf("Failed to parse authorized keys: %v", err)
		}
		if len(keys) != 1 {
			t.Fatalf("Expected 1 key, got %d", len(keys))
		}
		if string(keys[0].Marshal()) != string(pubKey.Marshal()) {
			t.Error("Parsed key does not match original")
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		_, priv2, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("Failed to generate second key: %v", err)
		}
		signer2, err := gossh.NewSignerFromKey(priv2)
		if err != nil {
			t.Fatalf("Failed to create second signer: %v", err)
		}
		authorizedKey2 := gossh.MarshalAuthorizedKey(signer2.PublicKey())

		combined := append(authorizedKey, authorizedKey2...)
		keys, err := pkgssh.ParseAuthorizedKeys(combined)
		if err != nil {
			t.Fatalf("Failed to parse authorized keys: %v", err)
		}
		if len(keys) != 2 {
			t.Fatalf("Expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("InvalidData", func(t *testing.T) {
		_, err := pkgssh.ParseAuthorizedKeys([]byte("invalid-key-data"))
		if err == nil {
			t.Error("Expected error for invalid key data")
		}
	})
}

func TestParseHostKeyFile(t *testing.T) {
	t.Run("InvalidData", func(t *testing.T) {
		_, err := pkgssh.ParseHostKeyFile([]byte("not-a-pem-key"))
		if err == nil {
			t.Error("Expected error for invalid key data")
		}
	})

	t.Run("ValidEd25519Key", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test_ed25519_key")
		signer, err := pkgssh.GenerateAndSaveHostKey(path, pkgssh.KeyTypeEd25519)
		if err != nil {
			t.Fatalf("GenerateAndSaveHostKey failed: %v", err)
		}
		if signer == nil {
			t.Fatal("Expected non-nil signer")
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("Failed to read key file: %v", err)
		}
		loaded, err := pkgssh.ParseHostKeyFile(data)
		if err != nil {
			t.Fatalf("ParseHostKeyFile failed: %v", err)
		}
		if string(loaded.PublicKey().Marshal()) != string(signer.PublicKey().Marshal()) {
			t.Error("Loaded key does not match generated key")
		}
	})
}

func TestGenerateAndSaveHostKey(t *testing.T) {
	t.Run("Ed25519", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test_ed25519_key")
		signer, err := pkgssh.GenerateAndSaveHostKey(path, pkgssh.KeyTypeEd25519)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if signer == nil {
			t.Fatal("Expected non-nil signer")
		}
		if signer.PublicKey().Type() != gossh.KeyAlgoED25519 {
			t.Errorf("Expected Ed25519 key type, got %s", signer.PublicKey().Type())
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Expected key file to exist at %s: %v", path, err)
		}
	})

	t.Run("RSA", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test_rsa_key")
		signer, err := pkgssh.GenerateAndSaveHostKey(path, pkgssh.KeyTypeRSA)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if signer == nil {
			t.Fatal("Expected non-nil signer")
		}
		if signer.PublicKey().Type() != gossh.KeyAlgoRSA {
			t.Errorf("Expected RSA key type, got %s", signer.PublicKey().Type())
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Expected key file to exist at %s: %v", path, err)
		}
	})

	t.Run("FilePermissions", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test_key")
		_, err := pkgssh.GenerateAndSaveHostKey(path, pkgssh.KeyTypeEd25519)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Failed to stat key file: %v", err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("Expected 0600 permissions, got %v", info.Mode().Perm())
		}
	})

	t.Run("InvalidPath", func(t *testing.T) {
		_, err := pkgssh.GenerateAndSaveHostKey("/nonexistent/dir/key", pkgssh.KeyTypeEd25519)
		if err == nil {
			t.Error("Expected error for invalid path")
		}
	})

	t.Run("UnsupportedKeyType", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test_key")
		_, err := pkgssh.GenerateAndSaveHostKey(path, pkgssh.KeyType("ecdsa"))
		if err == nil {
			t.Error("Expected error for unsupported key type")
		}
	})
}
