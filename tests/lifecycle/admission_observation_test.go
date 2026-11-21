package lifecycle_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestAdmissionUsesRequestFactsAndContemporaneousStatistics(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fact     lifecycle.JobFact
		stats    *lifecycle.DemandCounts
		assigned int
		state    string
		send     bool
	}{
		{"assigned and reflected", lifecycle.JobFact{RequestID: 100, Phase: lifecycle.JobAssigned}, &lifecycle.DemandCounts{Assigned: 1}, 1, "accepted", false},
		{"assigned with a stale sample", lifecycle.JobFact{RequestID: 100, Phase: lifecycle.JobAssigned}, nil, 1, "accepted", false},
		{"unrelated assignment", lifecycle.JobFact{RequestID: 99, Phase: lifecycle.JobAssigned}, &lifecycle.DemandCounts{Assigned: 1}, 1, "accepted", false},
		{"completion without timestamp", lifecycle.JobFact{RequestID: 100, Phase: lifecycle.JobCompleted}, nil, 1, "terminal", true},
		{"completion ahead of local clock", lifecycle.JobFact{RequestID: 100, Phase: lifecycle.JobCompleted, OccurredAt: admissionTime.Add(time.Millisecond)}, nil, 1, "terminal", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			now := admissionTime
			ledger, _, lease := admissionStore(t, now)
			const profile, session = "build", "owned"
			if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
				t.Fatal(err)
			}
			first := offer(session, 1, 100)
			expectDecision(t, ledger, lease, first, true, false, 100)
			if err := ledger.RecordAdmissionOutcome(ctx, lease, profile, now, first.Message, []int64{100}); err != nil {
				t.Fatal(err)
			}
			second := lifecycle.Delivery{MessageID: 2, Stats: tc.stats, Offers: []int64{101}, Jobs: []lifecycle.JobFact{tc.fact}}
			if err := ledger.PersistDelivery(ctx, lease, profile, session, now, second); err != nil {
				t.Fatal(err)
			}
			if got := acquisitionState(t, ledger, 100); got != tc.state {
				t.Fatalf("reservation state = %q, want %q", got, tc.state)
			}
			pending, err := ledger.PendingDelivery(ctx, "dev", profile, session, 2)
			if err != nil {
				t.Fatal(err)
			}
			decision, err := ledger.ReserveAdmission(ctx, lease, profile, now, lifecycle.AdmissionOffer{
				Message: pending.Receipt, RequestIDs: pending.Offers, EffectiveMax: 2, Assigned: tc.assigned,
			})
			if err != nil || decision.Send != tc.send {
				t.Fatalf("decision = %+v, want Send=%t, error = %v", decision, tc.send, err)
			}
		})
	}
}

func TestAccountingAndItsDemandSnapshotCommitTogether(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, db, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, &lifecycle.DemandCounts{}, now); err != nil {
		t.Fatal(err)
	}
	first := offer(session, 1, 100, 101)
	expectDecision(t, ledger, lease, first, true, false, 100, 101)
	if err := ledger.RecordAdmissionOutcome(ctx, lease, profile, now, first.Message, []int64{100, 101}); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("fixture did not commit the demand transaction")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, item := range input.TransactItems {
			if item.Put != nil && keyOf(item.Put.Item).SK == "DEMAND" {
				return interrupted
			}
		}
		return nil
	}
	delivery := lifecycle.Delivery{
		MessageID: 2, Stats: &lifecycle.DemandCounts{Assigned: 1}, Offers: []int64{102},
		Jobs: []lifecycle.JobFact{{RequestID: 100, Phase: lifecycle.JobAssigned}},
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted delivery: %v", err)
	}
	if got := acquisitionState(t, ledger, 100); got != "accepted" {
		t.Fatalf("reservation was released without durable demand: %q", got)
	}
	db.beforeWrite = nil
	delivery.Stats = nil
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
		t.Fatal(err)
	}
	pending, err := ledger.PendingDelivery(ctx, "dev", profile, session, 2)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ledger.ReserveAdmission(ctx, lease, profile, now, lifecycle.AdmissionOffer{
		Message: pending.Receipt, RequestIDs: pending.Offers, EffectiveMax: 2,
	})
	if err != nil || decision.Send {
		t.Fatalf("replay admitted extra work: %+v, error = %v", decision, err)
	}
}

func TestLaggingStatisticsCannotReleaseAssignedReservations(t *testing.T) {
	for _, count := range []int{2, 97} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := t.Context()
			now := admissionTime
			ledger, _, lease := admissionStore(t, now)
			const profile, session = "build", "owned"
			if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
				t.Fatal(err)
			}
			var jobs []lifecycle.JobFact
			for index := range count {
				id := int64(index + 1)
				first := offer(session, id, id)
				first.EffectiveMax = count + 1
				first.OutsideAssigned = 0
				decision, err := ledger.ReserveAdmission(ctx, lease, profile, now, first)
				if err != nil || !decision.Send {
					t.Fatalf("reserve %d: %+v, %v", id, decision, err)
				}
				if err := ledger.RecordAdmissionOutcome(ctx, lease, profile, now, first.Message, []int64{id}); err != nil {
					t.Fatal(err)
				}
				jobs = append(jobs, lifecycle.JobFact{RequestID: id, Phase: lifecycle.JobAssigned})
			}
			delivery := lifecycle.Delivery{MessageID: int64(count + 1), Stats: &lifecycle.DemandCounts{Assigned: 1}, Jobs: jobs}
			if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
				t.Fatal(err)
			}
			for _, job := range jobs {
				if got := acquisitionState(t, ledger, job.RequestID); got != "accepted" {
					t.Fatalf("lagging statistics released request %d into %q", job.RequestID, got)
				}
			}
		})
	}
}
