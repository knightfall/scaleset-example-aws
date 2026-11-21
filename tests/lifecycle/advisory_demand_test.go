package lifecycle_test

import (
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/listener"
)

func TestInconsistentStatisticsDoNotBlockJobFactsOrOpenAdmission(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	ledger, _, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, &lifecycle.DemandCounts{Assigned: 1}, now); err != nil {
		t.Fatal(err)
	}
	acked := false
	processor := listener.Processor{Store: ledger, MaxDemandAge: time.Minute, Session: deliverySession{
		id: session,
		poll: func(int64, int) (*lifecycle.Delivery, error) {
			return &lifecycle.Delivery{
				MessageID: 1, Stats: &lifecycle.DemandCounts{Assigned: 0, Running: 1}, Offers: []int64{100},
				Jobs: []lifecycle.JobFact{{RequestID: 200, Phase: lifecycle.JobCompleted, RunnerID: 7}},
			}, nil
		},
		acquire: func([]int64) ([]int64, error) {
			t.Fatal("inconsistent statistics opened admission")
			return nil, nil
		},
		acknowledge: func(int64) error {
			acked = true
			return nil
		},
	}}
	if err := processor.Step(ctx, lease, profile, &listener.Capacity{Maximum: 4}); err != nil {
		t.Fatal(err)
	}
	job, err := ledger.Get(ctx, lifecycle.ProfileKey("dev", profile, "JOB#200"))
	if err != nil || job == nil {
		t.Fatalf("job fact = %+v, error = %v", job, err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", profile)
	if err != nil || !acked || progress.Demand != nil {
		t.Fatalf("progress = %+v, acked = %t, error = %v", progress, acked, err)
	}
}

func TestInconsistentInitialStatisticsDoNotLoseOpenedSession(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	ledger, _, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	opening, err := ledger.BeginSessionOpen(ctx, lease, profile, target, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ConfirmSessionOpen(ctx, lease, profile, opening, session, &lifecycle.DemandCounts{Running: 1}, now); err != nil {
		t.Fatalf("opened session could not become durable: %v", err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", profile)
	if err != nil || progress.SessionID != session || progress.Demand != nil {
		t.Fatalf("progress = %+v, error = %v", progress, err)
	}
}
