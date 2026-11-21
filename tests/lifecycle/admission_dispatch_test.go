package lifecycle_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/listener"
)

func TestBeforeDispatchFailureReleasesCapacityForAReofferedRequest(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	ledger, db, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, &lifecycle.DemandCounts{}, now); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("fixture interrupted before dispatch commit")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, item := range input.TransactItems {
			if item.Put != nil && strings.HasPrefix(keyOf(item.Put.Item).SK, "MSG#") &&
				strings.Contains(stringValue(item.Put.Item["payload"]), `"dispatchPhase":"executing"`) {
				return interrupted
			}
		}
		return nil
	}
	var polls, sends, acks int64
	processor := listener.Processor{Store: ledger, MaxDemandAge: time.Minute, Session: deliverySession{
		id: session,
		poll: func(int64, int) (*lifecycle.Delivery, error) {
			polls++
			return &lifecycle.Delivery{MessageID: polls, Offers: []int64{100}}, nil
		},
		acquire: func(ids []int64) ([]int64, error) {
			sends++
			return ids, nil
		},
		acknowledge: func(int64) error {
			acks++
			return nil
		},
	}}
	capacity := &listener.Capacity{Maximum: 1}
	if err := processor.Step(ctx, lease, profile, capacity); !errors.Is(err, interrupted) || sends != 0 || acks != 0 {
		t.Fatalf("step before dispatch commit = %v with %d sends and %d acks", err, sends, acks)
	}
	db.beforeWrite = nil
	if err := processor.Step(ctx, lease, profile, capacity); err != nil || sends != 0 || acks != 1 {
		t.Fatalf("restart = %v with %d sends and %d acks", err, sends, acks)
	}
	if got := acquisitionState(t, ledger, 100); got != "unstarted" {
		t.Fatalf("cancelled acquisition = %q", got)
	}
	if err := processor.Step(ctx, lease, profile, capacity); err != nil || sends != 1 || acks != 2 {
		t.Fatalf("new offer = %v with %d sends and %d acks", err, sends, acks)
	}
	if got := acquisitionState(t, ledger, 100); got != "accepted" {
		t.Fatalf("reoffered acquisition = %q", got)
	}
}

func TestAcquisitionDispatchRemainsReservedWhenProviderOutcomeIsUnknown(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	first := offer("owned", 1, 100)
	expectDecision(t, ledger, lease, first, true, false, 100)
	if err := ledger.BeginAdmissionDispatch(ctx, lease, "build", now, first.Message); err != nil {
		t.Fatal(err)
	}
	if cancelled, err := ledger.CancelUnstartedAdmission(ctx, lease, "build", now, first.Message); err != nil || cancelled {
		t.Fatalf("cancel after begin = %t, error = %v", cancelled, err)
	}
	if err := ledger.RecordAdmissionOutcome(ctx, lease, "build", now, first.Message, nil); err != nil {
		t.Fatal(err)
	}
	item, err := ledger.Get(ctx, lifecycle.ProfileKey("dev", "build", "ACQUIRE#100"))
	if err != nil || item == nil || !item.DueAt.IsZero() || acquisitionState(t, ledger, 100) != "uncertain" {
		t.Fatalf("ambiguous acquisition entered the general effect recovery index: %+v, error = %v", item, err)
	}
}

func TestClosingSessionReleasesAnUnstartedAcquisition(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	const profile, session = "build", "old-session"
	deadline := now.Add(10 * time.Second)
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	opening, err := ledger.BeginSessionOpen(ctx, lease, profile, target, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := ledger.ConfirmSessionOpen(ctx, lease, profile, opening, session, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, lifecycle.Delivery{MessageID: 0, Offers: []int64{100}}); err != nil {
		t.Fatal(err)
	}
	pending, err := ledger.PendingDelivery(ctx, "dev", profile, session, 0)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ledger.ReserveAdmission(ctx, lease, profile, now, lifecycle.AdmissionOffer{
		Message: pending.Receipt, RequestIDs: pending.Offers, EffectiveMax: 1,
	})
	if err != nil || !decision.Send {
		t.Fatalf("reservation = %+v, error = %v", decision, err)
	}
	closing, err := ledger.BeginSessionClose(ctx, lease, profile, opened.OwnerName, session, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, profile, closing, now); err != nil {
		t.Fatal(err)
	}
	if got := acquisitionState(t, ledger, 100); got != "unstarted" {
		t.Fatalf("session closure left an unstarted reservation in %q", got)
	}
}

func TestClosingSessionPreservesDispatchedAcquisitionAsUncertain(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	const profile, session = "build", "old-session"
	deadline := now.Add(10 * time.Second)
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	opening, err := ledger.BeginSessionOpen(ctx, lease, profile, target, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := ledger.ConfirmSessionOpen(ctx, lease, profile, opening, session, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, lifecycle.Delivery{MessageID: 0, Offers: []int64{100}}); err != nil {
		t.Fatal(err)
	}
	pending, err := ledger.PendingDelivery(ctx, "dev", profile, session, 0)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ledger.ReserveAdmission(ctx, lease, profile, now, lifecycle.AdmissionOffer{
		Message: pending.Receipt, RequestIDs: pending.Offers, EffectiveMax: 1,
	})
	if err != nil || !decision.Send {
		t.Fatalf("reservation = %+v, error = %v", decision, err)
	}
	if err := ledger.BeginAdmissionDispatch(ctx, lease, profile, now, pending.Receipt); err != nil {
		t.Fatal(err)
	}
	closing, err := ledger.BeginSessionClose(ctx, lease, profile, opened.OwnerName, session, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, profile, closing, now); err != nil {
		t.Fatal(err)
	}
	if got := acquisitionState(t, ledger, 100); got != "uncertain" {
		t.Fatalf("session closure left an unstarted reservation in %q", got)
	}
}
