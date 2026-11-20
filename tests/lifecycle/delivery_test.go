package lifecycle_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/listener"
)

type deliverySession struct {
	id          string
	poll        func(int64, int) (*lifecycle.Delivery, error)
	acquire     func([]int64) ([]int64, error)
	acknowledge func(int64) error
}

func (s deliverySession) ID() string { return s.id }

func (s deliverySession) Poll(_ context.Context, cursor int64, capacity int) (*lifecycle.Delivery, error) {
	return s.poll(cursor, capacity)
}

func (s deliverySession) Acquire(_ context.Context, ids []int64) ([]int64, error) {
	return s.acquire(ids)
}

func (s deliverySession) Acknowledge(_ context.Context, id int64) error {
	return s.acknowledge(id)
}

func TestDeliveryProcessorResumesAcknowledgementWithoutReacquiring(t *testing.T) {
	now := time.Now()
	ledger, db, lease := admissionStore(t, now)
	ctx := t.Context()
	const sessionID = "owned-session"
	initial := &lifecycle.DemandCounts{Assigned: 1}
	if err := ledger.BeginDeliverySession(ctx, lease, "build", "", sessionID, initial, now); err != nil {
		t.Fatal(err)
	}
	capacity := &listener.Capacity{Maximum: 3}
	lostAck := errors.New("lost acknowledgement response")
	lostAcquire := errors.New("lost acquisition response")
	deliveries := []lifecycle.Delivery{
		{MessageID: 1, Offers: []int64{100, 101}, Jobs: []lifecycle.JobFact{{RequestID: 90, Phase: lifecycle.JobAssigned}}},
		{MessageID: 2, Stats: initial, Offers: []int64{102}, Jobs: []lifecycle.JobFact{{RequestID: 100, Phase: lifecycle.JobAssigned}}},
		{MessageID: 3, Stats: initial, Offers: []int64{102}, Jobs: []lifecycle.JobFact{
			{RequestID: 100, Phase: lifecycle.JobCompleted, RunnerID: 7, OccurredAt: now},
		}},
	}
	var polls, buys int
	var acks []int64
	remote := deliverySession{
		id: sessionID,
		poll: func(cursor int64, maximum int) (*lifecycle.Delivery, error) {
			if cursor != int64(polls) || maximum != 3 || polls >= len(deliveries) {
				t.Fatalf("unexpected poll %d with cursor %d and maximum %d", polls, cursor, maximum)
			}
			delivery := deliveries[polls]
			polls++
			return &delivery, nil
		},
		acquire: func(ids []int64) ([]int64, error) {
			buys++
			record, err := ledger.Get(ctx, lifecycle.ProfileKey("dev", "build", "JOB#90"))
			if err != nil || record == nil {
				t.Fatalf("job facts missing before acquisition: %v", err)
			}
			if buys == 1 && slices.Equal(ids, []int64{100, 101}) {
				return []int64{100}, nil
			}
			if buys == 2 && polls == 3 && slices.Equal(ids, []int64{102}) {
				return nil, lostAcquire
			}
			t.Fatalf("unexpected acquisition %v after poll %d", ids, polls)
			return nil, nil
		},
		acknowledge: func(id int64) error {
			acks = append(acks, id)
			if id == 1 {
				if acquisitionState(t, ledger, 100) != "accepted" || acquisitionState(t, ledger, 101) != "uncertain" {
					t.Fatal("acknowledgement ran before the acquisition outcome was durable")
				}
				if len(acks) == 1 {
					return lostAck
				}
			}
			if id == 3 && acquisitionState(t, ledger, 102) != "uncertain" {
				t.Fatal("acknowledgement ran before the lost acquisition was recorded")
			}
			return nil
		},
	}
	processor := listener.Processor{Store: ledger, Session: remote, MaxDemandAge: time.Minute}
	if err := processor.Step(ctx, lease, "build", capacity); !errors.Is(err, lostAck) {
		t.Fatalf("first delivery = %v", err)
	}
	pending, err := ledger.SessionProgress(ctx, "dev", "build")
	if err != nil || pending.PendingMessageID != 1 || pending.LastAcknowledged != 0 ||
		pending.Demand == nil || !pending.Demand.ObservedAt.Equal(now) {
		t.Fatalf("pending progress = %+v, error = %v", pending, err)
	}
	processor.Store, err = lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := processor.Step(ctx, lease, "build", capacity); err != nil {
			t.Fatal(err)
		}
	}
	if err := processor.Step(ctx, lease, "build", capacity); !errors.Is(err, lostAcquire) {
		t.Fatalf("lost acquisition = %v", err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", "build")
	if err != nil || progress.LastAcknowledged != 3 || progress.PendingMessageID != 0 ||
		progress.Demand == nil || progress.Demand.Assigned != 1 || progress.Demand.MessageID != 3 ||
		polls != 3 || buys != 2 || !slices.Equal(acks, []int64{1, 1, 2, 3}) {
		t.Fatalf("progress = %+v, error = %v, polls = %d, acquisitions = %d, acks = %v", progress, err, polls, buys, acks)
	}
	if got := acquisitionState(t, ledger, 100); got != "terminal" {
		t.Fatalf("verified completion left state %q", got)
	}
}

func TestDeliveryProcessorClosesAdmissionForAbsentOrStaleDemand(t *testing.T) {
	for _, tc := range []struct {
		name     string
		observed bool
	}{{"absent", false}, {"stale", true}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			ledger, _, lease := admissionStore(t, now)
			var initial *lifecycle.DemandCounts
			if tc.observed {
				initial = &lifecycle.DemandCounts{}
			}
			if err := ledger.BeginDeliverySession(t.Context(), lease, "build", "", "owned", initial, now.Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
			acked := false
			processor := listener.Processor{
				Store: ledger, MaxDemandAge: time.Second,
				Session: deliverySession{
					id: "owned",
					poll: func(int64, int) (*lifecycle.Delivery, error) {
						return &lifecycle.Delivery{MessageID: 1, Offers: []int64{10}}, nil
					},
					acquire: func([]int64) ([]int64, error) {
						t.Fatal("admission opened without valid demand")
						return nil, nil
					},
					acknowledge: func(int64) error {
						acked = true
						return nil
					},
				},
			}
			if err := processor.Step(t.Context(), lease, "build", &listener.Capacity{Maximum: 4}); err != nil {
				t.Fatal(err)
			}
			if !acked {
				t.Fatal("safe no-admission decision did not acknowledge")
			}
		})
	}
}

func TestDeliveryCannotAdvanceCursorBeforeDurableAcknowledgementIntent(t *testing.T) {
	now := time.Now()
	ledger, _, lease := admissionStore(t, now)
	ctx := t.Context()
	if err := ledger.BeginDeliverySession(ctx, lease, "build", "", "owned", nil, now); err != nil {
		t.Fatal(err)
	}
	delivery := lifecycle.Delivery{MessageID: 1}
	if err := ledger.PersistDelivery(ctx, lease, "build", "owned", now, delivery); err != nil {
		t.Fatal(err)
	}
	if err := ledger.AuthorizeAcknowledgement(ctx, lease, "build", "owned", 1, now); err == nil {
		t.Fatal("authorized ack before recording the admission outcome")
	}
	if err := ledger.ConfirmAcknowledgement(ctx, lease, "build", "owned", 1, now); err == nil {
		t.Fatal("advanced cursor without an ack intent")
	}
	delivery.Offers = []int64{30}
	if err := ledger.PersistDelivery(ctx, lease, "build", "owned", now, delivery); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("changed replay error = %v", err)
	}
	if err := ledger.BeginDeliverySession(ctx, lease, "build", "another-session", "successor", nil, now); !errors.Is(err, lifecycle.ErrSessionChanged) {
		t.Fatalf("replacement with wrong prior session = %v", err)
	}
}

func TestConflictingRunnerFactsRemainProtected(t *testing.T) {
	now := time.Now()
	ledger, _, lease := admissionStore(t, now)
	ctx := t.Context()
	if err := ledger.BeginDeliverySession(ctx, lease, "build", "", "owned", &lifecycle.DemandCounts{}, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, "build", "owned", now, lifecycle.Delivery{MessageID: 1, Offers: []int64{100}}); err != nil {
		t.Fatal(err)
	}
	delivery, err := ledger.PendingDelivery(ctx, "dev", "build", "owned", 1)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ledger.ReserveAdmission(ctx, lease, "build", now, lifecycle.AdmissionOffer{
		Message: delivery.Receipt, RequestIDs: delivery.Offers, EffectiveMax: 1,
	})
	if err != nil || !decision.Send {
		t.Fatalf("first decision = %+v, error = %v", decision, err)
	}
	if err := ledger.RecordAdmissionOutcome(ctx, lease, "build", now, delivery.Receipt, []int64{100}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.AuthorizeAcknowledgement(ctx, lease, "build", "owned", 1, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ConfirmAcknowledgement(ctx, lease, "build", "owned", 1, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, "build", "owned", now, lifecycle.Delivery{
		MessageID: 2, Jobs: []lifecycle.JobFact{
			{RequestID: 100, Phase: lifecycle.JobCompleted, RunnerID: 7, OccurredAt: now},
			{RequestID: 100, Phase: lifecycle.JobStarted, RunnerID: 8, OccurredAt: now.Add(-time.Second)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	record, err := ledger.Get(ctx, lifecycle.ProfileKey("dev", "build", "JOB#100"))
	if err != nil || record == nil {
		t.Fatalf("job record: %v", err)
	}
	var job struct {
		lifecycle.JobFact
		ConflictingRunner bool `json:"conflictingRunner"`
	}
	if err := json.Unmarshal(record.Payload, &job); err != nil {
		t.Fatal(err)
	}
	if job.Phase != lifecycle.JobCompleted || !job.ConflictingRunner || acquisitionState(t, ledger, 100) != "accepted" {
		t.Fatalf("contradictory observations lost their protection: %+v", job)
	}
}
