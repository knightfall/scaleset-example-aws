package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type InboxMessage struct {
	SessionID  string    `json:"sessionId"`
	MessageID  int64     `json:"messageId"`
	Digest     string    `json:"digest"`
	ReceivedAt time.Time `json:"receivedAt"`
}

// RecordInbox returns false for a replay of the same durable delivery. Receipt
// is distinct from acquisition handling and never authorizes acknowledgement.
func (s *Store) RecordInbox(ctx context.Context, deployment, profile string, message InboxMessage) (bool, error) {
	digest, err := hex.DecodeString(message.Digest)
	if err != nil || len(digest) != sha256.Size || deployment == "" || profile == "" ||
		message.SessionID == "" || strings.Contains(message.SessionID, "#") || message.MessageID < 0 || message.ReceivedAt.IsZero() {
		return false, errors.New("invalid lifecycle inbox message")
	}
	message.Digest = hex.EncodeToString(digest)
	key := ProfileKey(deployment, profile, "MSG#"+message.SessionID+"#"+strconv.FormatInt(message.MessageID, 10))
	payload, err := json.Marshal(message)
	if err != nil {
		return false, fmt.Errorf("encode inbox message: %w", err)
	}
	err = s.Apply(ctx, "", Change{Record: Record{Key: key, Deployment: deployment, Payload: payload}})
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, ErrConflict) {
		return false, err
	}
	record, err := s.Get(ctx, key)
	if err != nil {
		return false, err
	}
	var existing InboxMessage
	if record == nil || record.Deployment != deployment || json.Unmarshal(record.Payload, &existing) != nil ||
		existing.SessionID != message.SessionID || existing.MessageID != message.MessageID || existing.Digest != message.Digest {
		return false, fmt.Errorf("%w: inbox delivery identity or content differs", ErrConflict)
	}
	return false, nil
}

type Acquisition struct {
	RequestID int64  `json:"requestId"`
	State     string `json:"state"`
}

// AcquisitionChange prepares a reservation update for a transaction with its
// corresponding inbox state. Request IDs remain scoped to the profile.
func AcquisitionChange(deployment, profile string, acquisition Acquisition, expectedRevision int64) (Change, error) {
	var change Change
	if deployment == "" || profile == "" || acquisition.RequestID <= 0 {
		return change, errors.New("invalid acquisition identity")
	}
	switch acquisition.State {
	case "pending", "uncertain", "accepted", "accounted", "terminal":
	default:
		return change, errors.New("invalid acquisition state")
	}
	payload, err := json.Marshal(acquisition)
	if err != nil {
		return change, fmt.Errorf("encode acquisition: %w", err)
	}
	return Change{
		Record: Record{
			Key:        ProfileKey(deployment, profile, "ACQUIRE#"+strconv.FormatInt(acquisition.RequestID, 10)),
			Deployment: deployment, Payload: payload,
		},
		ExpectedRevision: expectedRevision,
	}, nil
}
