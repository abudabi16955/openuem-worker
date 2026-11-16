package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const (
	// KeySize is the required AES-256 key size in bytes.
	KeySize = 32
	// NonceSize is the required AES-GCM nonce size in bytes.
	NonceSize = 12
)

// EncryptRecoveryPassword encrypts a BitLocker recovery password using AES-256-GCM.
// It generates a cryptographically secure 12-byte nonce and returns a base64-encoded
// string composed of: base64(nonce_12B || ciphertext || tag_16B).
func EncryptRecoveryPassword(plainText string, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", fmt.Errorf("vault key must be exactly %d bytes, got %d", KeySize, len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("could not create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("could not create GCM cipher: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("could not generate nonce: %w", err)
	}

	// gcm.Seal appends the ciphertext and tag to dst. Passing nonce as dst creates: nonce || ciphertext || tag.
	sealed := gcm.Seal(nonce, nonce, []byte(plainText), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// DecryptRecoveryPassword decrypts a base64-encoded string (nonce_12B || ciphertext || tag_16B)
// encrypted with AES-256-GCM using the provided 32-byte key.
func DecryptRecoveryPassword(cipherTextBase64 string, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", fmt.Errorf("vault key must be exactly %d bytes, got %d", KeySize, len(key))
	}

	raw, err := base64.StdEncoding.DecodeString(cipherTextBase64)
	if err != nil {
		return "", fmt.Errorf("invalid base64 ciphertext: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("could not create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("could not create GCM cipher: %w", err)
	}

	nonceSize := gcm.NonceSize()
	minLen := nonceSize + gcm.Overhead()
	if len(raw) < minLen {
		return "", errors.New("ciphertext too short")
	}

	nonce := raw[:nonceSize]
	ciphertextAndTag := raw[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertextAndTag, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
	}

	return string(plaintext), nil
}

// ValidateVaultKey decodes a base64 string and validates that it produces exactly 32 bytes.
func ValidateVaultKey(encodedKey string) ([]byte, error) {
	trimmed := strings.TrimSpace(encodedKey)
	if trimmed == "" {
		return nil, errors.New("vault key string is empty")
	}

	key, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		// Fallback to raw standard encoding in case padding was omitted
		if rawKey, rawErr := base64.RawStdEncoding.DecodeString(trimmed); rawErr == nil {
			key = rawKey
		} else {
			return nil, fmt.Errorf("vault key is not valid base64: %w", err)
		}
	}

	if len(key) != KeySize {
		return nil, fmt.Errorf("vault key must decode to exactly %d bytes, got %d", KeySize, len(key))
	}

	return key, nil
}

// GetVaultKey reads the OPENUEM_VAULT_KEY environment variable and validates it.
func GetVaultKey() ([]byte, error) {
	envKey := os.Getenv("OPENUEM_VAULT_KEY")
	if envKey == "" {
		return nil, errors.New("OPENUEM_VAULT_KEY environment variable is not set")
	}

	return ValidateVaultKey(envKey)
}

// MustGetVaultKey validates the OPENUEM_VAULT_KEY environment variable, logging a fatal
// error and terminating with exit code 1 if missing or invalid.
func MustGetVaultKey() []byte {
	key, err := GetVaultKey()
	if err != nil {
		log.Fatalf("[FATAL]: OPENUEM_VAULT_KEY validation failed: %v", err)
	}
	return key
}
