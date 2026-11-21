package lifecycle_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestDeliveryReplayAfterInterruptedWriteUsesStableFacts(t *testing.T) {
	now := time.Now()
	ledger, db, lease := admissionStore(t, now)
	ctx := t.Context()
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("fixture interrupted after receipt")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, item := range input.TransactItems {
			if item.Put != nil && keyOf(item.Put.Item).SK == "DEMAND" {
				return interrupted
			}
		}
		return nil
	}
	delivery := lifecycle.Delivery{
		MessageID: 1, Stats: &lifecycle.DemandCounts{Assigned: 1}, Offers: []int64{19},
		Jobs: []lifecycle.JobFact{{RequestID: 11, Phase: lifecycle.JobStarted, RunnerID: 7}},
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); !errors.Is(err, interrupted) {
		t.Fatalf("first write = %v", err)
	}
	db.beforeWrite = nil

	delivery.Stats.Assigned++
	replayed := delivery
	replayed.Offers = []int64{20}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, replayed); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("changed job offers returned %v", err)
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
		t.Fatalf("same message with fresh statistics: %v", err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", profile)
	if err != nil || progress.PendingMessageID != 1 || progress.Demand == nil || progress.Demand.Assigned != 2 {
		t.Fatalf("resumed progress = %+v, error = %v", progress, err)
	}
}

func TestDeliveryAcceptsExactReplayOfLegacyReceipt(t *testing.T) {
	now := time.Now()
	ledger, _, lease := admissionStore(t, now)
	ctx := t.Context()
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
		t.Fatal(err)
	}
	delivery := lifecycle.Delivery{
		MessageID: 1, Stats: &lifecycle.DemandCounts{Assigned: 1}, Offers: []int64{19},
	}
	legacy, err := json.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	message := lifecycle.InboxMessage{
		SessionID: session, MessageID: delivery.MessageID,
		Digest: fmt.Sprintf("%x", sha256.Sum256(legacy)), ReceivedAt: now,
	}
	if _, err := ledger.RecordInbox(ctx, "dev", profile, message); err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
		t.Fatalf("valid legacy replay returned %v", err)
	}
	pending, err := ledger.PendingDelivery(ctx, "dev", profile, session, delivery.MessageID)
	if err != nil || pending.Receipt.Digest != message.Digest {
		t.Fatalf("legacy pending receipt = %+v, error = %v", pending.Receipt, err)
	}
}
