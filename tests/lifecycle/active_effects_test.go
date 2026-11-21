package lifecycle_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestUnfinishedEffectsUsesProfilePagesAndExactJournalState(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 33; index++ {
		if _, err := ledger.PlanEffect(ctx, lease, "build", journalEffect(fmt.Sprintf("build-%02d", index), now), now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ledger.PlanEffect(ctx, lease, "other", journalEffect("foreign", now), now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginEffect(ctx, lease, "build", "build-32", now); err != nil {
		t.Fatal(err)
	}
	before := db.effectPages
	work, cursor, err := ledger.UnfinishedEffects(ctx, lease, "build", lifecycle.Key{}, now)
	if err != nil || len(work) != 32 || work[0].ID != "build-00" || work[31].ID != "build-31" || cursor == (lifecycle.Key{}) {
		t.Fatalf("first active page: %+v cursor %+v error %v", work, cursor, err)
	}
	work, end, err := ledger.UnfinishedEffects(ctx, lease, "build", cursor, now)
	if err != nil || len(work) != 1 || work[0].ID != "build-32" || work[0].State != lifecycle.EffectExecuting || end != (lifecycle.Key{}) {
		t.Fatalf("last active page: %+v cursor %+v error %v", work, end, err)
	}
	if _, _, err := ledger.UnfinishedEffects(ctx, lease, "other", cursor, now); err == nil {
		t.Fatal("accepted another profile's cursor")
	}
	if db.effectPages != before {
		t.Fatal("index pagination read the settled journal")
	}
}

func TestSettlingEffectRemovesActiveMarkerAtomically(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	intent := journalEffect("cancel-me", now)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("active marker delete interrupted")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, item := range input.TransactItems {
			if item.Delete != nil {
				return sentinel
			}
		}
		return nil
	}
	if _, err := ledger.CancelPlannedEffect(ctx, lease, "build", intent.ID, now); !errors.Is(err, sentinel) {
		t.Fatalf("failed marker delete did not stop the transition: %v", err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	work, _, err := ledger.UnfinishedEffects(ctx, lease, "build", lifecycle.Key{}, now)
	if err != nil || len(work) != 1 || work[0].State != lifecycle.EffectPlanned {
		t.Fatalf("failed settlement changed pending work: %+v, %v", work, err)
	}
	db.beforeWrite = nil
	if cancelled, err := ledger.CancelPlannedEffect(ctx, lease, "build", intent.ID, now); err != nil || !cancelled {
		t.Fatalf("cancel: %t, %v", cancelled, err)
	}
	work, _, err = ledger.UnfinishedEffects(ctx, lease, "build", lifecycle.Key{}, now)
	if err != nil || len(work) != 0 {
		t.Fatalf("cancelled work still indexed: %+v, %v", work, err)
	}
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	work, _, err = ledger.UnfinishedEffects(ctx, lease, "build", lifecycle.Key{}, now)
	if err != nil || len(work) != 1 || work[0].ID != intent.ID || work[0].State != lifecycle.EffectPlanned {
		t.Fatalf("rearmed work not indexed: %+v, %v", work, err)
	}
}

func TestDurableCoordinationRejectsTTL(t *testing.T) {
	ledger, _ := store(t)
	for _, key := range []lifecycle.Key{
		{PK: "ENV#dev", SK: "CATALOG"},
		lifecycle.ProfileKey("dev", "build", "SESSION"),
		lifecycle.ProfileKey("dev", "build", "DEMAND"),
		lifecycle.ProfileKey("dev", "build", "ADMISSION"),
		lifecycle.ProfileKey("dev", "build", "EFFECTS"),
		lifecycle.ProfileKey("dev", "build", "EFFECT-REBUILD"),
		lifecycle.ProfileKey("dev", "build", "ACTIVE-OP#1#pending"),
		lifecycle.ProfileKey("dev", "build", "RUNNER#allocation"),
		lifecycle.ProfileKey("dev", "build", "RUNNER-ID#123"),
	} {
		row := record("dev", key, "{}")
		row.ExpiresAt = admissionTime
		if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: row}); err == nil {
			t.Fatalf("TTL was accepted for %s", key.SK)
		}
	}
	row := record("dev", lifecycle.ProfileKey("dev", "build", "OP#ambiguous"), `{"id":"ambiguous","state":"uncertain"}`)
	row.ExpiresAt = admissionTime
	if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: row}); err == nil {
		t.Fatal("TTL was accepted for an unscheduled unfinished effect")
	}
}
