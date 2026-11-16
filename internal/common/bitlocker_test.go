package common

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

func startTestNatsServer(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port for nats: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command("nats-server", "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start nats-server: %v", err)
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
	}
	if !ready {
		_ = cmd.Process.Kill()
		t.Fatalf("nats-server did not become ready in time on %s", url)
	}

	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	return url, cleanup
}

func TestBitLockerEscrowHandler_RequestReply_Success(t *testing.T) {
	natsURL, cleanupNATS := startTestNatsServer(t)
	defer cleanupNATS()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to test nats: %v", err)
	}
	defer nc.Close()

	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	worker := &Worker{
		Model:          &models.Model{Client: client},
		VaultKey:       vaultKey,
		NATSConnection: nc,
	}

	sub, err := nc.QueueSubscribe(openuem_nats.BitLockerEscrowSubjectPattern, "openuem-agents", worker.BitLockerEscrowHandler)
	if err != nil {
		t.Fatalf("failed to queue subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("failed to flush nats: %v", err)
	}

	agentID := "agent-req-reply-1"
	req := openuem_nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "C:",
		KeyID:            "{KEY-RR-111}",
		RecoveryPassword: "123456-123456-123456-123456",
		CreatedAt:        time.Now(),
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal req: %v", err)
	}

	subject := openuem_nats.BitLockerEscrowSubject(agentID)
	respMsg, err := nc.Request(subject, reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("nc.Request failed: %v", err)
	}

	var resp openuem_nats.BitLockerEscrowResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("failed to unmarshal BitLockerEscrowResponse: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected escrow response success true, got false (error: %s)", resp.Error)
	}
	if resp.Error != "" {
		t.Fatalf("expected empty error in success response, got %s", resp.Error)
	}

	// Verify key in DB
	key, err := worker.Model.GetActiveBitLockerKey(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get active key from DB: %v", err)
	}
	if key.KeyID != "{KEY-RR-111}" {
		t.Fatalf("expected key ID {KEY-RR-111}, got %s", key.KeyID)
	}
}

func TestBitLockerEscrowHandler_RequestReply_InvalidJSON_Error(t *testing.T) {
	natsURL, cleanupNATS := startTestNatsServer(t)
	defer cleanupNATS()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to test nats: %v", err)
	}
	defer nc.Close()

	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	worker := &Worker{
		Model:          &models.Model{Client: client},
		VaultKey:       make([]byte, crypto.KeySize),
		NATSConnection: nc,
	}

	sub, err := nc.QueueSubscribe(openuem_nats.BitLockerEscrowSubjectPattern, "openuem-agents", worker.BitLockerEscrowHandler)
	if err != nil {
		t.Fatalf("failed to queue subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	_ = nc.Flush()

	agentID := "agent-req-reply-err"
	subject := openuem_nats.BitLockerEscrowSubject(agentID)
	respMsg, err := nc.Request(subject, []byte("invalid json payload"), 5*time.Second)
	if err != nil {
		t.Fatalf("nc.Request failed: %v", err)
	}

	var resp openuem_nats.BitLockerEscrowResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("failed to unmarshal BitLockerEscrowResponse: %v", err)
	}

	if resp.Success {
		t.Fatalf("expected escrow response success false, got true")
	}
	if resp.Error == "" {
		t.Fatalf("expected non-empty error in error response")
	}
}

func TestBitLockerEscrowHandler_RequestReply_DBValidationError(t *testing.T) {
	natsURL, cleanupNATS := startTestNatsServer(t)
	defer cleanupNATS()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to test nats: %v", err)
	}
	defer nc.Close()

	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	rand.Read(vaultKey)

	worker := &Worker{
		Model:          &models.Model{Client: client},
		VaultKey:       vaultKey,
		NATSConnection: nc,
	}

	sub, err := nc.QueueSubscribe(openuem_nats.BitLockerEscrowSubjectPattern, "openuem-agents", worker.BitLockerEscrowHandler)
	if err != nil {
		t.Fatalf("failed to queue subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	_ = nc.Flush()

	agentID := "agent-req-reply-validation"
	// Missing KeyID and RecoveryPassword
	req := openuem_nats.BitLockerEscrowRequest{
		AgentID:     agentID,
		DriveLetter: "C:",
	}
	reqData, _ := json.Marshal(req)

	subject := openuem_nats.BitLockerEscrowSubject(agentID)
	respMsg, err := nc.Request(subject, reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("nc.Request failed: %v", err)
	}

	var resp openuem_nats.BitLockerEscrowResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("failed to unmarshal BitLockerEscrowResponse: %v", err)
	}

	if resp.Success {
		t.Fatalf("expected success false for invalid escrow request, got true")
	}
	if resp.Error == "" {
		t.Fatalf("expected error message for missing fields")
	}
}

type testACLCluster struct {
	port                int
	ConnectWorker       func(t *testing.T, opts ...nats.Option) *nats.Conn
	ConnectAgent        func(t *testing.T, opts ...nats.Option) *nats.Conn
	ConnectUnauthorized func(t *testing.T, opts ...nats.Option) *nats.Conn
}

func startTestNatsServerWithACL(t *testing.T) (*testACLCluster, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port for nats: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	conf := fmt.Sprintf(`
port: %d
authorization {
  users = [
    {
      user: "worker"
      password: "worker-password"
      permissions {
        sub {
          allow: [
            "_INBOX.>",
            "report openuem-agents",
            "deployresult openuem-agents",
            "ping.agentworker openuem-agents",
            "agentconfig openuem-agents",
            "wingetcfg.> openuem-agents",
            "ansiblecfg.> openuem-agents",
            "agent.bitlocker.escrow.* openuem-agents",
            "agent.bitlocker.rotate.* openuem-agents"
          ]
        }
      }
    },
    {
      user: "agent"
      password: "agent-password"
      permissions {
        sub {
          allow: [
            "_INBOX.>",
            "agent.update.>",
            "agent.uninstall.>",
            "agentrollback.>",
            "agent.certificate",
            "agent.newconfig",
            "agent.installpackage.>",
            "agent.uninstallpackage.>",
            "agent.updatepackage.>",
            "agent.enable.>",
            "agent.disable.>",
            "agent.report.>",
            "agent.installpackage.>",
            "agent.settings.>",
            "agent.startvnc.> openuem-agent-management",
            "agent.stopvnc.> openuem-agent-management",
            "agent.restart.> openuem-agent-management",
            "agent.poweroff.> openuem-agent-management",
            "agent.reboot.> openuem-agent-management",
            "agent.removeprinter.> openuem-agent-management",
            "agent.defaultprinter.> openuem-agent-management",
            "agent.rustdesk.> openuem-agent-management",
            "agent.startsftp.> openuem-agent-management",
            "agent.netbird.> openuem-agent-management",
            "agent.ping.> openuem-agent-management",
            "agent.ansible.> openuem-agent-management",
            "agent.windowstask.> openuem-agent-management",
            "agent.runprofile.> openuem-agent-management",
            "agent.bitlocker.force_escrow.>",
            "agent.bitlocker.rotate.>"
          ]
        }
        publish {
          allow: [
            "report",
            "_INBOX.>",
            "deployresult",
            "agentconfig",
            "wingetcfg.>",
            "ansiblecfg.>",
            "$JS.API.STREAM.INFO.AGENTS_STREAM",
            "$JS.API.CONSUMER.CREATE.>",
            "$JS.API.CONSUMER.MSG.NEXT.>",
            "$JS.ACK.>",
            "$JS.NACK.>",
            "agent.bitlocker.escrow.>",
            "agent.bitlocker.rotate.>"
          ]
        }
      }
    },
    {
      user: "unauthorized"
      password: "unauthorized-password"
      permissions {
        sub {
          allow: ["_INBOX.>"]
        }
        publish {
          allow: ["_INBOX.>"]
        }
      }
    }
  ]
}
`, port)

	tmpFile, err := os.CreateTemp("", "nats-acl-*.conf")
	if err != nil {
		t.Fatalf("failed to create temp config file for nats: %v", err)
	}
	if _, err := tmpFile.WriteString(conf); err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("failed to write nats acl config: %v", err)
	}
	tmpFile.Close()

	cmd := exec.Command("nats-server", "-c", tmpFile.Name())
	if err := cmd.Start(); err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("failed to start nats-server with ACL: %v", err)
	}

	ready := false
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
	}
	if !ready {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		os.Remove(tmpFile.Name())
		t.Fatalf("nats-server with ACL did not become ready in time on port %d", port)
	}

	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		os.Remove(tmpFile.Name())
	}

	cluster := &testACLCluster{
		port: port,
		ConnectWorker: func(t *testing.T, opts ...nats.Option) *nats.Conn {
			t.Helper()
			nc, err := nats.Connect(fmt.Sprintf("nats://worker:worker-password@127.0.0.1:%d", port), opts...)
			if err != nil {
				t.Fatalf("failed to connect worker to nats with ACL: %v", err)
			}
			return nc
		},
		ConnectAgent: func(t *testing.T, opts ...nats.Option) *nats.Conn {
			t.Helper()
			nc, err := nats.Connect(fmt.Sprintf("nats://agent:agent-password@127.0.0.1:%d", port), opts...)
			if err != nil {
				t.Fatalf("failed to connect agent to nats with ACL: %v", err)
			}
			return nc
		},
		ConnectUnauthorized: func(t *testing.T, opts ...nats.Option) *nats.Conn {
			t.Helper()
			nc, err := nats.Connect(fmt.Sprintf("nats://unauthorized:unauthorized-password@127.0.0.1:%d", port), opts...)
			if err != nil {
				t.Fatalf("failed to connect unauthorized user to nats with ACL: %v", err)
			}
			return nc
		},
	}

	return cluster, cleanup
}

func TestBitLockerEscrowHandler_ACL_Success(t *testing.T) {
	cluster, cleanup := startTestNatsServerWithACL(t)
	defer cleanup()

	workerNC := cluster.ConnectWorker(t)
	defer workerNC.Close()

	client := enttest.Open(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	defer client.Close()

	vaultKey := make([]byte, crypto.KeySize)
	if _, err := rand.Read(vaultKey); err != nil {
		t.Fatalf("failed to generate random vault key: %v", err)
	}

	worker := &Worker{
		Model:          &models.Model{Client: client},
		VaultKey:       vaultKey,
		NATSConnection: workerNC,
	}

	// 1. Worker queue subscribes to escrow and rotate subjects with openuem-agents queue
	escrowSub, err := workerNC.QueueSubscribe(openuem_nats.BitLockerEscrowSubjectPattern, "openuem-agents", worker.BitLockerEscrowHandler)
	if err != nil {
		t.Fatalf("worker failed to queue subscribe to escrow: %v", err)
	}
	defer escrowSub.Unsubscribe()

	rotateSub, err := workerNC.QueueSubscribe(openuem_nats.BitLockerRotateSubjectPattern, "openuem-agents", worker.BitLockerRotateHandler)
	if err != nil {
		t.Fatalf("worker failed to queue subscribe to rotate: %v", err)
	}
	defer rotateSub.Unsubscribe()

	if err := workerNC.Flush(); err != nil {
		t.Fatalf("failed to flush worker connection: %v", err)
	}

	// 2. Agent connects with ACL credentials and publishes escrow request
	agentNC := cluster.ConnectAgent(t)
	defer agentNC.Close()

	agentID := "agent-acl-verified-1"
	escrowReq := openuem_nats.BitLockerEscrowRequest{
		AgentID:          agentID,
		DriveLetter:      "C:",
		KeyID:            "{KEY-ACL-SUCCESS-1}",
		RecoveryPassword: "123456-654321-123456-654321",
		CreatedAt:        time.Now(),
	}
	reqData, err := json.Marshal(escrowReq)
	if err != nil {
		t.Fatalf("failed to marshal escrow request: %v", err)
	}

	subject := openuem_nats.BitLockerEscrowSubject(agentID)
	respMsg, err := agentNC.Request(subject, reqData, 5*time.Second)
	if err != nil {
		t.Fatalf("agentNC.Request failed under ACL: %v", err)
	}

	var resp openuem_nats.BitLockerEscrowResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("failed to unmarshal BitLockerEscrowResponse: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected escrow response success true, got false (error: %s)", resp.Error)
	}
	if resp.Error != "" {
		t.Fatalf("expected empty error in success response, got %s", resp.Error)
	}

	// Verify key saved and active in DB
	key, err := worker.Model.GetActiveBitLockerKey(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get active key from DB: %v", err)
	}
	if key.KeyID != "{KEY-ACL-SUCCESS-1}" {
		t.Fatalf("expected KeyID {KEY-ACL-SUCCESS-1}, got %s", key.KeyID)
	}
	if !key.IsActive {
		t.Fatal("expected key to be active")
	}

	// Verify key password decrypts correctly
	decrypted, err := crypto.DecryptRecoveryPassword(key.EncryptedPassword, vaultKey)
	if err != nil || decrypted != escrowReq.RecoveryPassword {
		t.Fatalf("failed to decrypt password: %v, got %q", err, decrypted)
	}

	// 3. Agent publishes rotate message under ACL
	rotateReq := openuem_nats.BitLockerRotateConfirmation{
		AgentID:     agentID,
		DriveLetter: "C:",
		OldKeyID:    "{KEY-ACL-SUCCESS-1}",
		NewKeyID:    "{KEY-ACL-ROTATED-2}",
		RotatedAt:   time.Now(),
	}
	rotData, err := json.Marshal(rotateReq)
	if err != nil {
		t.Fatalf("failed to marshal rotate confirmation: %v", err)
	}

	if err := agentNC.Publish(openuem_nats.BitLockerRotateSubject(agentID), rotData); err != nil {
		t.Fatalf("agentNC.Publish failed under ACL: %v", err)
	}
	if err := agentNC.Flush(); err != nil {
		t.Fatalf("failed to flush agent connection after publish: %v", err)
	}

	// Allow handler to process
	time.Sleep(100 * time.Millisecond)

	// Verify old key deactivated
	history, err := worker.Model.GetBitLockerKeyHistory(agentID, "C:")
	if err != nil {
		t.Fatalf("failed to get key history: %v", err)
	}
	for _, k := range history {
		if k.KeyID == "{KEY-ACL-SUCCESS-1}" && k.IsActive {
			t.Fatal("expected key {KEY-ACL-SUCCESS-1} to be deactivated after rotate")
		}
	}
}

func TestBitLocker_ACL_PermissionsViolation(t *testing.T) {
	cluster, cleanup := startTestNatsServerWithACL(t)
	defer cleanup()

	t.Run("UnauthorizedUser_PublishToEscrowDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		unauthNC := cluster.ConnectUnauthorized(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer unauthNC.Close()

		_ = unauthNC.Publish("agent.bitlocker.escrow.agent-test", []byte("unauthorized escrow"))
		_ = unauthNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected publish permissions violation error, but none was received")
		}
	})

	t.Run("UnauthorizedUser_SubscribeToEscrowDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		unauthNC := cluster.ConnectUnauthorized(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer unauthNC.Close()

		_, _ = unauthNC.Subscribe("agent.bitlocker.escrow.*", func(m *nats.Msg) {})
		_ = unauthNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected subscribe permissions violation error, but none was received")
		}
	})

	t.Run("Worker_SubscribeWithoutQueueGroupDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		workerNC := cluster.ConnectWorker(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer workerNC.Close()

		// Worker is only allowed: "agent.bitlocker.escrow.* openuem-agents"
		// Subscribing without queue group must violate ACL
		_, _ = workerNC.Subscribe("agent.bitlocker.escrow.*", func(m *nats.Msg) {})
		_ = workerNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error without queue group, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected permissions violation error for subscribing without queue group, but none received")
		}
	})

	t.Run("Worker_SubscribeWithWrongQueueGroupDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		workerNC := cluster.ConnectWorker(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer workerNC.Close()

		_, _ = workerNC.QueueSubscribe("agent.bitlocker.escrow.*", "wrong-queue", func(m *nats.Msg) {})
		_ = workerNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error with wrong queue group, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected permissions violation error with wrong queue group, but none received")
		}
	})

	t.Run("Agent_PublishToForbiddenSubjectDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		agentNC := cluster.ConnectAgent(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer agentNC.Close()

		// Agent has sub.allow for force_escrow, but NOT publish permission
		_ = agentNC.Publish("agent.bitlocker.force_escrow.agent-test", []byte("forbidden"))
		_ = agentNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error for publishing to forbidden subject, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected permissions violation error for agent publishing to forbidden subject, but none received")
		}
	})

	t.Run("Agent_SubscribeToForbiddenSubjectDenied", func(t *testing.T) {
		errCh := make(chan error, 5)
		agentNC := cluster.ConnectAgent(t, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			errCh <- err
		}))
		defer agentNC.Close()

		// Agent has publish for "report", but NOT sub permission
		_, _ = agentNC.Subscribe("report", func(m *nats.Msg) {})
		_ = agentNC.Flush()

		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("expected permissions violation error for subscribing to forbidden subject, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected permissions violation error for agent subscribing to forbidden subject, but none received")
		}
	})
}
