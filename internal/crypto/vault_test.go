package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"os"
	"os/exec"
	"testing"
)

func TestEncryptDecryptRecoveryPassword_Success(t *testing.T) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	passwords := []string{
		"111111-222222-333333-444444-555555-666666-777777-888888",
		"zażółć gęślą jaźń 12345!@#$%",
		"",
		"a very long recovery key that might contain multiple sentences or characters ...",
	}

	for _, pwd := range passwords {
		encrypted, err := EncryptRecoveryPassword(pwd, key)
		if err != nil {
			t.Fatalf("encryption failed for %q: %v", pwd, err)
		}

		decrypted, err := DecryptRecoveryPassword(encrypted, key)
		if err != nil {
			t.Fatalf("decryption failed for %q: %v", pwd, err)
		}

		if decrypted != pwd {
			t.Fatalf("decrypted text mismatch: expected %q, got %q", pwd, decrypted)
		}
	}
}

func TestEncryptRecoveryPassword_NonceRandomness(t *testing.T) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	pwd := "48-digit-bitlocker-recovery-key-123456"

	enc1, err := EncryptRecoveryPassword(pwd, key)
	if err != nil {
		t.Fatalf("enc1 failed: %v", err)
	}

	enc2, err := EncryptRecoveryPassword(pwd, key)
	if err != nil {
		t.Fatalf("enc2 failed: %v", err)
	}

	if enc1 == enc2 {
		t.Fatalf("two encryptions of the same plaintext must produce different ciphertexts (random nonce), got identical")
	}

	// Verify both decrypt to the same plaintext
	dec1, err := DecryptRecoveryPassword(enc1, key)
	if err != nil || dec1 != pwd {
		t.Fatalf("dec1 mismatch: %v, %q", err, dec1)
	}

	dec2, err := DecryptRecoveryPassword(enc2, key)
	if err != nil || dec2 != pwd {
		t.Fatalf("dec2 mismatch: %v, %q", err, dec2)
	}
}

func TestEncryptDecrypt_InvalidKeySize(t *testing.T) {
	shortKey := make([]byte, 16) // AES-128, but 32 bytes required
	longKey := make([]byte, 64)

	pwd := "secret-recovery-password"

	if _, err := EncryptRecoveryPassword(pwd, shortKey); err == nil {
		t.Fatal("expected error with 16-byte key in EncryptRecoveryPassword")
	}
	if _, err := EncryptRecoveryPassword(pwd, longKey); err == nil {
		t.Fatal("expected error with 64-byte key in EncryptRecoveryPassword")
	}
	if _, err := EncryptRecoveryPassword(pwd, nil); err == nil {
		t.Fatal("expected error with nil key in EncryptRecoveryPassword")
	}

	validKey := make([]byte, KeySize)
	enc, err := EncryptRecoveryPassword(pwd, validKey)
	if err != nil {
		t.Fatalf("failed to encrypt with valid key: %v", err)
	}

	if _, err := DecryptRecoveryPassword(enc, shortKey); err == nil {
		t.Fatal("expected error with 16-byte key in DecryptRecoveryPassword")
	}
	if _, err := DecryptRecoveryPassword(enc, longKey); err == nil {
		t.Fatal("expected error with 64-byte key in DecryptRecoveryPassword")
	}
}

func TestDecryptRecoveryPassword_WrongKeyAndCorruptedCiphertext(t *testing.T) {
	key1 := make([]byte, KeySize)
	key2 := make([]byte, KeySize)
	rand.Read(key1)
	rand.Read(key2)

	pwd := "123456-654321"
	enc, err := EncryptRecoveryPassword(pwd, key1)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// Decrypting with wrong key must fail authentication
	if _, err := DecryptRecoveryPassword(enc, key2); err == nil {
		t.Fatal("expected authentication error when decrypting with wrong key")
	}

	// Invalid base64
	if _, err := DecryptRecoveryPassword("not!valid@base64", key1); err == nil {
		t.Fatal("expected error for invalid base64")
	}

	// Ciphertext too short
	shortCipher := base64.StdEncoding.EncodeToString([]byte("too-short"))
	if _, err := DecryptRecoveryPassword(shortCipher, key1); err == nil {
		t.Fatal("expected error for short ciphertext")
	}

	// Corrupted ciphertext payload
	raw, _ := base64.StdEncoding.DecodeString(enc)
	raw[len(raw)-1] ^= 0xFF // flip last bit of authentication tag
	tampered := base64.StdEncoding.EncodeToString(raw)
	if _, err := DecryptRecoveryPassword(tampered, key1); err == nil {
		t.Fatal("expected authentication error for tampered ciphertext")
	}
}

func TestValidateVaultKeyAndGetVaultKey(t *testing.T) {
	// Valid 32-byte key base64
	raw32 := make([]byte, 32)
	for i := range raw32 {
		raw32[i] = byte(i + 1)
	}
	validB64 := base64.StdEncoding.EncodeToString(raw32)

	key, err := ValidateVaultKey(validB64)
	if err != nil {
		t.Fatalf("unexpected error for valid key: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(key))
	}

	// Invalid cases
	invalidCases := []struct {
		name string
		val  string
	}{
		{"empty", ""},
		{"whitespace", "   "},
		{"not base64", "%%%not-base64%%%"},
		{"16 bytes", base64.StdEncoding.EncodeToString(make([]byte, 16))},
		{"31 bytes", base64.StdEncoding.EncodeToString(make([]byte, 31))},
		{"33 bytes", base64.StdEncoding.EncodeToString(make([]byte, 33))},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ValidateVaultKey(tc.val); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}

	// Test GetVaultKey with environment variable
	origEnv := os.Getenv("OPENUEM_VAULT_KEY")
	defer os.Setenv("OPENUEM_VAULT_KEY", origEnv)

	os.Unsetenv("OPENUEM_VAULT_KEY")
	if _, err := GetVaultKey(); err == nil {
		t.Fatal("expected error when OPENUEM_VAULT_KEY is unset")
	}

	os.Setenv("OPENUEM_VAULT_KEY", validB64)
	retrieved, err := GetVaultKey()
	if err != nil {
		t.Fatalf("failed to get valid vault key from env: %v", err)
	}
	if len(retrieved) != 32 {
		t.Fatalf("expected 32 bytes from env, got %d", len(retrieved))
	}
}

func TestMustGetVaultKey_ExitOnMissingOrInvalidKey(t *testing.T) {
	if os.Getenv("BE_VAULT_KEY_CRASHER") == "1" {
		MustGetVaultKey()
		return
	}

	testCases := []struct {
		name   string
		envVal string
		setEnv bool
	}{
		{name: "unset", envVal: "", setEnv: false},
		{name: "invalid_base64", envVal: "not-base-64!!", setEnv: true},
		{name: "invalid_length", envVal: base64.StdEncoding.EncodeToString([]byte("short")), setEnv: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestMustGetVaultKey_ExitOnMissingOrInvalidKey")
			cmd.Env = append(os.Environ(), "BE_VAULT_KEY_CRASHER=1")
			if tc.setEnv {
				cmd.Env = append(cmd.Env, "OPENUEM_VAULT_KEY="+tc.envVal)
			} else {
				// Remove OPENUEM_VAULT_KEY from env
				filtered := make([]string, 0, len(cmd.Env))
				for _, e := range cmd.Env {
					if len(e) >= 18 && e[:18] == "OPENUEM_VAULT_KEY=" {
						continue
					}
					filtered = append(filtered, e)
				}
				cmd.Env = filtered
			}

			err := cmd.Run()
			if e, ok := err.(*exec.ExitError); ok && !e.Success() {
				return // Expected exit error
			}
			t.Fatalf("process ran with err %v, want exit status 1", err)
		})
	}
}
