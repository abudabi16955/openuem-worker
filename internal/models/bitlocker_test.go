package models

import (
	"crypto/rand"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/open-uem/ent/enttest"
	"github.com/open-uem/nats"
	"github.com/open-uem/openuem-worker/internal/crypto"
)

func TestSaveBitLockerEscrowAndRotation(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	m := Model{Client: client}

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	agentID := "agent-test-uuid-1"

	// 1. Initial escrow for drive C:
	req1 := &nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "C:",
		KeyID:            "{KEY-1111-1111}",
		RecoveryPassword: "111111-222222-333333-444444",
		CreatedAt:        time.Now().Add(-1 * time.Hour),
	}

	saved1, err := m.SaveBitLockerEscrow(req1, vaultKey)
	if err != nil {
		t.Fatalf("failed to save initial escrow: %v", err)
	}

	if saved1.AgentID != agentID || saved1.KeyID != "{KEY-1111-1111}" || !saved1.IsActive {
		t.Fatalf("unexpected saved1 properties: %+v", saved1)
	}

	// Verify password was properly encrypted
	decrypted1, err := crypto.DecryptRecoveryPassword(saved1.EncryptedPassword, vaultKey)
	if err != nil || decrypted1 != req1.RecoveryPassword {
		t.Fatalf("failed to decrypt saved1 password: %v, %q", err, decrypted1)
	}

	// 2. Escrow second key for same drive C: (rotation)
	req2 := &nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "c",
		KeyID:            "{KEY-2222-2222}",
		RecoveryPassword: "555555-666666-777777-888888",
		CreatedAt:        time.Now(),
	}

	saved2, err := m.SaveBitLockerEscrow(req2, vaultKey)
	if err != nil {
		t.Fatalf("failed to save rotated escrow: %v", err)
	}

	if saved2.KeyID != "{KEY-2222-2222}" || !saved2.IsActive {
		t.Fatalf("unexpected saved2: %+v", saved2)
	}

	// Verify active key is now KEY-2222-2222
	activeKey, err := m.GetActiveBitLockerKey(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get active key: %v", err)
	}
	if activeKey.KeyID != "{KEY-2222-2222}" {
		t.Fatalf("expected active key {KEY-2222-2222}, got %s", activeKey.KeyID)
	}

	// Verify history contains both keys
	history, err := m.GetBitLockerKeyHistory(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 keys in history, got %d", len(history))
	}

	// First key should now be inactive
	for _, k := range history {
		if k.KeyID == "{KEY-1111-1111}" && k.IsActive {
			t.Fatalf("expected key 1111 to be inactive, but is_active=true")
		}
	}

	// 3. Test SaveBitLockerRotate
	conf := &nats.BitLockerRotateConfirmation{
		AgentID:     agentID,
		DriveLetter: "C:",
		OldKeyID:    "{KEY-2222-2222}",
		NewKeyID:    "{KEY-3333-3333}",
		RotatedAt:   time.Now(),
	}
	if err := m.SaveBitLockerRotate(conf); err != nil {
		t.Fatalf("failed to save rotate confirmation: %v", err)
	}

	// Verify KEY-2222 is now inactive
	historyAfterRotate, err := m.GetBitLockerKeyHistory(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get history after rotate: %v", err)
	}
	for _, k := range historyAfterRotate {
		if k.KeyID == "{KEY-2222-2222}" && k.IsActive {
			t.Fatalf("expected key 2222 to be inactive after rotate confirmation")
		}
	}
}

func TestSaveBitLockerEscrow_Validation(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()
	m := Model{Client: client}

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	// nil request
	if _, err := m.SaveBitLockerEscrow(nil, vaultKey); err == nil {
		t.Fatal("expected error on nil request")
	}

	// empty agent_id
	if _, err := m.SaveBitLockerEscrow(&nats.BitLockerEscrowRequest{
		KeyID:            "key-1",
		RecoveryPassword: "pwd",
	}, vaultKey); err == nil {
		t.Fatal("expected error on empty agent_id")
	}

	// empty key_id
	if _, err := m.SaveBitLockerEscrow(&nats.BitLockerEscrowRequest{
		AgentID:          "agent-1",
		RecoveryPassword: "pwd",
	}, vaultKey); err == nil {
		t.Fatal("expected error on empty key_id")
	}

	// empty recovery_password
	if _, err := m.SaveBitLockerEscrow(&nats.BitLockerEscrowRequest{
		AgentID: "agent-1",
		KeyID:   "key-1",
	}, vaultKey); err == nil {
		t.Fatal("expected error on empty recovery_password")
	}

	// invalid vault key
	if _, err := m.SaveBitLockerEscrow(&nats.BitLockerEscrowRequest{
		AgentID:          "agent-1",
		KeyID:            "key-1",
		RecoveryPassword: "pwd",
	}, []byte("short-key")); err == nil {
		t.Fatal("expected error on invalid vault key")
	}
}
