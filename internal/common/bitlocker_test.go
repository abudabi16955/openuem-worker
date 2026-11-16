package common

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nats-io/nats.go"
	"github.com/open-uem/ent/enttest"
	openuem_nats "github.com/open-uem/nats"
	"github.com/open-uem/openuem-worker/internal/crypto"
	"github.com/open-uem/openuem-worker/internal/models"
)

func TestBitLockerEscrowHandler_SuccessAndRotation(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	if _, err := rand.Read(vaultKey); err != nil {
		t.Fatalf("failed to generate random vault key: %v", err)
	}

	worker := &Worker{
		Model:    &models.Model{Client: client},
		VaultKey: vaultKey,
	}

	agentID := "agent-uuid-42"

	// 1. Initial escrow via NATS message
	req1 := openuem_nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "C:",
		KeyID:            "{KEY-AAA-111}",
		RecoveryPassword: "111111-222222-333333-444444",
		CreatedAt:        time.Now().Add(-10 * time.Minute),
	}
	data1, err := json.Marshal(req1)
	if err != nil {
		t.Fatalf("failed to marshal req1: %v", err)
	}

	msg1 := &nats.Msg{
		Subject: openuem_nats.BitLockerEscrowSubject(agentID),
		Data:    data1,
	}
	worker.BitLockerEscrowHandler(msg1)

	// Verify key saved and active
	key1, err := worker.Model.GetActiveBitLockerKey(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get active bitlocker key: %v", err)
	}
	if key1.KeyID != "{KEY-AAA-111}" {
		t.Fatalf("expected KeyID {KEY-AAA-111}, got %s", key1.KeyID)
	}
	if !key1.IsActive {
		t.Fatal("expected key1 to be active")
	}

	// Verify decrypted password
	decrypted1, err := crypto.DecryptRecoveryPassword(key1.EncryptedPassword, vaultKey)
	if err != nil || decrypted1 != req1.RecoveryPassword {
		t.Fatalf("failed to decrypt password: %v, got %q", err, decrypted1)
	}

	// 2. Second escrow with new key on same drive (key rotation)
	req2 := openuem_nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "c:",
		KeyID:            "{KEY-BBB-222}",
		RecoveryPassword: "555555-666666-777777-888888",
		CreatedAt:        time.Now(),
	}
	data2, err := json.Marshal(req2)
	if err != nil {
		t.Fatalf("failed to marshal req2: %v", err)
	}

	msg2 := &nats.Msg{
		Subject: openuem_nats.BitLockerEscrowSubject(agentID),
		Data:    data2,
	}
	worker.BitLockerEscrowHandler(msg2)

	// Verify key2 is now active and key1 is inactive
	key2, err := worker.Model.GetActiveBitLockerKey(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get active bitlocker key: %v", err)
	}
	if key2.KeyID != "{KEY-BBB-222}" {
		t.Fatalf("expected active key {KEY-BBB-222}, got %s", key2.KeyID)
	}

	history, err := worker.Model.GetBitLockerKeyHistory(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 keys in history, got %d", len(history))
	}
	for _, k := range history {
		if k.KeyID == "{KEY-AAA-111}" && k.IsActive {
			t.Fatal("expected older key {KEY-AAA-111} to be deactivated")
		}
	}
}

func TestBitLockerEscrowHandler_ExtractAgentIDFromSubject(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	worker := &Worker{
		Model:    &models.Model{Client: client},
		VaultKey: vaultKey,
	}

	agentID := "agent-uuid-from-subject"
	// Request without agent_id in payload
	req := openuem_nats.BitLockerEscrowRequest{
		DriveLetter:      "D:",
		KeyID:            "{KEY-DDD-999}",
		RecoveryPassword: "999999-888888-777777-666666",
	}
	data, _ := json.Marshal(req)

	msg := &nats.Msg{
		Subject: "agent.bitlocker.escrow." + agentID,
		Data:    data,
	}
	worker.BitLockerEscrowHandler(msg)

	activeKey, err := worker.Model.GetActiveBitLockerKey(agentID, "D:")
	if err != nil {
		t.Fatalf("expected key to be found under subject agentID: %v", err)
	}
	if activeKey.AgentID != agentID {
		t.Fatalf("expected agent_id %s, got %s", agentID, activeKey.AgentID)
	}
}

func TestBitLockerRotateHandler(t *testing.T) {
	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	worker := &Worker{
		Model:    &models.Model{Client: client},
		VaultKey: vaultKey,
	}

	agentID := "agent-rotate-test"

	// First save a key
	req := &openuem_nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "C:",
		KeyID:            "{KEY-OLD}",
		RecoveryPassword: "password-1",
	}
	_, err := worker.Model.SaveBitLockerEscrow(req, vaultKey)
	if err != nil {
		t.Fatalf("failed to save initial key: %v", err)
	}

	// Send rotate confirmation
	conf := openuem_nats.BitLockerRotateConfirmation{
		AgentID:     agentID,
		DriveLetter: "C:",
		OldKeyID:    "{KEY-OLD}",
		NewKeyID:    "{KEY-NEW}",
		RotatedAt:   time.Now(),
	}
	data, _ := json.Marshal(conf)
	msg := &nats.Msg{
		Subject: openuem_nats.BitLockerRotateSubject(agentID),
		Data:    data,
	}
	worker.BitLockerRotateHandler(msg)

	// Verify old key is inactive
	history, err := worker.Model.GetBitLockerKeyHistory(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	for _, k := range history {
		if k.KeyID == "{KEY-OLD}" && k.IsActive {
			t.Fatal("expected old key to be deactivated after rotate message")
		}
	}
}
