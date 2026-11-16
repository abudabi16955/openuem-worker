package models

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/open-uem/ent"
	"github.com/open-uem/ent/bitlockerkey"
	"github.com/open-uem/nats"
	"github.com/open-uem/openuem-worker/internal/crypto"
)

// normalizeDriveLetter ensures consistent drive letter format, defaulting to "C:".
func normalizeDriveLetter(drive string) string {
	d := strings.ToUpper(strings.TrimSpace(drive))
	if d == "" {
		return "C:"
	}
	if len(d) == 1 && d[0] >= 'A' && d[0] <= 'Z' {
		return d + ":"
	}
	return d
}

// SaveBitLockerEscrow encrypts the recovery password using AES-256-GCM with the vault key,
// deactivates any existing active keys for this agent and drive letter, and saves the new BitlockerKey.
func (m *Model) SaveBitLockerEscrow(req *nats.BitLockerEscrowRequest, vaultKey []byte) (*ent.BitlockerKey, error) {
	if req == nil {
		return nil, errors.New("BitLockerEscrowRequest is nil")
	}
	if strings.TrimSpace(req.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	if strings.TrimSpace(req.KeyID) == "" {
		return nil, errors.New("key_id is required")
	}
	if strings.TrimSpace(req.RecoveryPassword) == "" {
		return nil, errors.New("recovery_password is required")
	}

	encryptedPassword, err := crypto.EncryptRecoveryPassword(req.RecoveryPassword, vaultKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt recovery password: %w", err)
	}

	driveLetter := normalizeDriveLetter(req.DriveLetter)
	ctx := context.Background()

	tx, err := m.Client.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	// Check if this key_id already exists for this agent
	existingKey, err := tx.BitlockerKey.Query().
		Where(
			bitlockerkey.AgentIDEQ(req.AgentID),
			bitlockerkey.KeyIDEQ(req.KeyID),
		).
		First(ctx)

	if err != nil && !ent.IsNotFound(err) {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to query existing bitlocker key: %w", err)
	}

	if existingKey != nil {
		// Deactivate other active keys for this agent and drive
		_, err = tx.BitlockerKey.Update().
			Where(
				bitlockerkey.AgentIDEQ(req.AgentID),
				bitlockerkey.DriveLetterEQ(driveLetter),
				bitlockerkey.KeyIDNEQ(req.KeyID),
				bitlockerkey.IsActiveEQ(true),
			).
			SetIsActive(false).
			Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to deactivate older bitlocker keys: %w", err)
		}

		// Update existing key
		saved, err := tx.BitlockerKey.UpdateOne(existingKey).
			SetDriveLetter(driveLetter).
			SetEncryptedPassword(encryptedPassword).
			SetIsActive(true).
			SetUpdatedAt(time.Now()).
			Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to update bitlocker key: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit bitlocker escrow transaction: %w", err)
		}
		return saved, nil
	}

	// Deactivate any currently active keys for this drive on this agent
	_, err = tx.BitlockerKey.Update().
		Where(
			bitlockerkey.AgentIDEQ(req.AgentID),
			bitlockerkey.DriveLetterEQ(driveLetter),
			bitlockerkey.IsActiveEQ(true),
		).
		SetIsActive(false).
		Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to deactivate previous active keys: %w", err)
	}

	// Create new active BitlockerKey record
	create := tx.BitlockerKey.Create().
		SetAgentID(req.AgentID).
		SetDriveLetter(driveLetter).
		SetKeyID(req.KeyID).
		SetEncryptedPassword(encryptedPassword).
		SetIsActive(true)

	if !req.CreatedAt.IsZero() {
		create.SetCreatedAt(req.CreatedAt)
	}

	saved, err := create.Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to create bitlocker key: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit bitlocker escrow transaction: %w", err)
	}

	return saved, nil
}

// SaveBitLockerRotate handles key rotation confirmation by deactivating the old key
// and ensuring any new key or subsequent active key is properly marked active.
func (m *Model) SaveBitLockerRotate(conf *nats.BitLockerRotateConfirmation) error {
	if conf == nil {
		return errors.New("BitLockerRotateConfirmation is nil")
	}
	if strings.TrimSpace(conf.AgentID) == "" {
		return errors.New("agent_id is required")
	}

	ctx := context.Background()
	driveLetter := normalizeDriveLetter(conf.DriveLetter)

	tx, err := m.Client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start database transaction: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	// Deactivate old key if OldKeyID was supplied
	if strings.TrimSpace(conf.OldKeyID) != "" {
		_, err = tx.BitlockerKey.Update().
			Where(
				bitlockerkey.AgentIDEQ(conf.AgentID),
				bitlockerkey.KeyIDEQ(conf.OldKeyID),
			).
			SetIsActive(false).
			Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to deactivate old bitlocker key %s: %w", conf.OldKeyID, err)
		}
	}

	// If NewKeyID was supplied, deactivate other active keys for this drive and activate NewKeyID if present
	if strings.TrimSpace(conf.NewKeyID) != "" {
		_, err = tx.BitlockerKey.Update().
			Where(
				bitlockerkey.AgentIDEQ(conf.AgentID),
				bitlockerkey.DriveLetterEQ(driveLetter),
				bitlockerkey.KeyIDNEQ(conf.NewKeyID),
				bitlockerkey.IsActiveEQ(true),
			).
			SetIsActive(false).
			Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to deactivate prior keys during rotation: %w", err)
		}

		_, err = tx.BitlockerKey.Update().
			Where(
				bitlockerkey.AgentIDEQ(conf.AgentID),
				bitlockerkey.KeyIDEQ(conf.NewKeyID),
			).
			SetIsActive(true).
			Save(ctx)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to activate new bitlocker key %s: %w", conf.NewKeyID, err)
		}
	}

	return tx.Commit()
}

// GetActiveBitLockerKey returns the current active BitlockerKey for an agent and drive letter.
func (m *Model) GetActiveBitLockerKey(agentID, driveLetter string) (*ent.BitlockerKey, error) {
	driveLetter = normalizeDriveLetter(driveLetter)
	return m.Client.BitlockerKey.Query().
		Where(
			bitlockerkey.AgentIDEQ(agentID),
			bitlockerkey.DriveLetterEQ(driveLetter),
			bitlockerkey.IsActiveEQ(true),
		).
		First(context.Background())
}

// GetBitLockerKeyHistory returns all BitLocker keys (both active and rotated/inactive) for an agent.
func (m *Model) GetBitLockerKeyHistory(agentID, driveLetter string) ([]*ent.BitlockerKey, error) {
	query := m.Client.BitlockerKey.Query().Where(bitlockerkey.AgentIDEQ(agentID))
	if driveLetter != "" {
		query = query.Where(bitlockerkey.DriveLetterEQ(normalizeDriveLetter(driveLetter)))
	}
	return query.Order(ent.Desc(bitlockerkey.FieldCreatedAt)).All(context.Background())
}
