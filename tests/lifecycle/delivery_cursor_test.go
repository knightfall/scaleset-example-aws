package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/listener"
)

func TestProcessorResumesMessageZeroAfterLostAcknowledgement(t *testing.T) {
	now := time.Now()
	ledger, db, lease := admissionStore(t, now)
	ctx := t.Context()
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ConfirmAcknowledgement(ctx, lease, profile, session, 0, now); !errors.Is(err, lifecycle.ErrSessionChanged) {
		t.Fatalf("initial empty cursor accepted confirmation for zero: %v", err)
	}
	lostAck := errors.New("fixture lost acknowledgement")
	var polls, acks int
	processor := listener.Processor{Store: ledger, MaxDemandAge: time.Minute, Session: deliverySession{
		id: session,
		poll: func(cursor int64, _ int) (*lifecycle.Delivery, error) {
			polls++
			switch polls {
			case 1:
				return &lifecycle.Delivery{MessageID: 0}, nil
			case 2:
				return &lifecycle.Delivery{MessageID: 1}, nil
			default:
				t.Fatalf("unexpected poll with cursor %d", cursor)
				return nil, nil
			}
		},
		acknowledge: func(id int64) error {
			acks++
			if (acks < 3 && id != 0) || (acks == 3 && id != 1) {
				t.Fatalf("acknowledgement %d used ID %d", acks, id)
			}
			if acks == 1 {
				return lostAck
			}
			return nil
		},
	}}
	if err := processor.Step(ctx, lease, profile, nil); !errors.Is(err, lostAck) {
		t.Fatalf("first step: %v", err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", profile)
	if err != nil || !progress.HasPendingMessage() || progress.PendingMessageID != 0 {
		t.Fatalf("pending progress = %+v, error = %v", progress, err)
	}
	processor.Store, err = lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Step(ctx, lease, profile, nil); err != nil {
		t.Fatalf("replayed step: %v", err)
	}
	if polls != 1 || acks != 2 {
		t.Fatalf("unexpected replay: %d polls, %d acknowledgements", polls, acks)
	}
	if err := ledger.ConfirmAcknowledgement(ctx, lease, profile, session, 0, now); err != nil {
		t.Fatalf("repeated confirmation for zero: %v", err)
	}
	if err := processor.Step(ctx, lease, profile, nil); err != nil {
		t.Fatalf("following message: %v", err)
	}
	progress, err = ledger.SessionProgress(ctx, "dev", profile)
	if err != nil || progress.HasPendingMessage() || progress.LastAcknowledged != 1 || polls != 2 || acks != 3 {
		t.Fatalf("final progress = %+v, error = %v, %d polls, %d acknowledgements", progress, err, polls, acks)
	}
}
