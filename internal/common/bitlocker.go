package common

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	openuem_nats "github.com/open-uem/nats"
	"github.com/open-uem/openuem-worker/internal/crypto"
)

// SubscribeToBitLockerWorkerQueues registers subscriptions for BitLocker escrow and rotation.
func (w *Worker) SubscribeToBitLockerWorkerQueues() error {
	_, err := w.NATSConnection.QueueSubscribe(openuem_nats.BitLockerEscrowSubjectPattern, "openuem-agents", w.BitLockerEscrowHandler)
	if err != nil {
		log.Printf("[ERROR]: could not subscribe to %s NATS message, reason: %v", openuem_nats.BitLockerEscrowSubjectPattern, err)
		return err
	}
	log.Printf("[INFO]: subscribed to message %s", openuem_nats.BitLockerEscrowSubjectPattern)

	_, err = w.NATSConnection.QueueSubscribe(openuem_nats.BitLockerRotateSubjectPattern, "openuem-agents", w.BitLockerRotateHandler)
	if err != nil {
		log.Printf("[ERROR]: could not subscribe to %s NATS message, reason: %v", openuem_nats.BitLockerRotateSubjectPattern, err)
		return err
	}
	log.Printf("[INFO]: subscribed to message %s", openuem_nats.BitLockerRotateSubjectPattern)

	return nil
}

// BitLockerEscrowHandler handles incoming BitLocker recovery key escrow reports from agents.
func (w *Worker) BitLockerEscrowHandler(msg *nats.Msg) {
	respond := func(success bool, errMsg string) {
		if msg.Reply != "" {
			resp := openuem_nats.BitLockerEscrowResponse{
				Success: success,
				Error:   errMsg,
			}
			respBytes, err := json.Marshal(resp)
			if err == nil {
				if rErr := msg.Respond(respBytes); rErr != nil {
					log.Printf("[WARN]: failed to send BitLocker escrow response on reply %s: %v", msg.Reply, rErr)
				}
			}
		}
	}

	if len(w.VaultKey) == 0 {
		key, err := crypto.GetVaultKey()
		if err != nil {
			log.Printf("[ERROR]: cannot handle BitLocker escrow, vault key not configured: %v", err)
			respond(false, "vault key not configured: "+err.Error())
			return
		}
		w.VaultKey = key
	}

	var escrowReq openuem_nats.BitLockerEscrowRequest
	if err := json.Unmarshal(msg.Data, &escrowReq); err != nil {
		log.Printf("[ERROR]: could not unmarshal BitLocker escrow request: %v", err)
		respond(false, "could not unmarshal BitLocker escrow request: "+err.Error())
		return
	}

	if escrowReq.AgentID == "" {
		parts := strings.Split(msg.Subject, ".")
		if len(parts) >= 4 {
			escrowReq.AgentID = parts[3]
		}
	}

	saved, err := w.Model.SaveBitLockerEscrow(&escrowReq, w.VaultKey)
	if err != nil {
		log.Printf("[ERROR]: could not save BitLocker escrow for agent %s: %v", escrowReq.AgentID, err)
		respond(false, "could not save BitLocker escrow: "+err.Error())
		return
	}

	log.Printf("[INFO]: successfully escrowed BitLocker key %s for agent %s drive %s (id: %d)",
		saved.KeyID, saved.AgentID, saved.DriveLetter, saved.ID)
	respond(true, "")
}

// BitLockerRotateHandler handles incoming BitLocker recovery key rotation confirmations from agents.
func (w *Worker) BitLockerRotateHandler(msg *nats.Msg) {
	var rotateConf openuem_nats.BitLockerRotateConfirmation
	if err := json.Unmarshal(msg.Data, &rotateConf); err != nil {
		log.Printf("[ERROR]: could not unmarshal BitLocker rotate confirmation: %v", err)
		return
	}

	if rotateConf.AgentID == "" {
		parts := strings.Split(msg.Subject, ".")
		if len(parts) >= 4 {
			rotateConf.AgentID = parts[3]
		}
	}

	if err := w.Model.SaveBitLockerRotate(&rotateConf); err != nil {
		log.Printf("[ERROR]: could not save BitLocker rotate confirmation for agent %s: %v", rotateConf.AgentID, err)
		return
	}

	log.Printf("[INFO]: successfully processed BitLocker rotate confirmation for agent %s (old: %s, new: %s)",
		rotateConf.AgentID, rotateConf.OldKeyID, rotateConf.NewKeyID)
}
