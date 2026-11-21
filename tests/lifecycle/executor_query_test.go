package lifecycle_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestExecutorQueryReadsStrongBoundedPagesAndExcludesOtherRecords(t *testing.T) {
	ledger, db, lease := admissionStore(t, admissionTime)
	for _, allocation := range []string{"a", "b"} {
		if err := ledger.BindExecutor(t.Context(), lease, "build", lifecycle.ExecutorBinding{
			AllocationID: allocation, ResourceID: "resource-" + allocation, Incarnation: "boot-" + allocation,
		}, admissionTime); err != nil {
			t.Fatal(err)
		}
	}
	first := lifecycle.ProfileKey("dev", "build", "RUNNER#a")
	second := lifecycle.ProfileKey("dev", "build", "RUNNER#b")
	var calls int
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		if !aws.ToBool(input.ConsistentRead) || !aws.ToBool(input.ScanIndexForward) || input.IndexName != nil ||
			aws.ToInt32(input.Limit) != 1 || stringValue(input.ExpressionAttributeValues[":prefix"]) != "RUNNER#" ||
			stringValue(input.ExpressionAttributeValues[":pk"]) != first.PK {
			return nil, errors.New("expected a bounded ascending allocation query on the base table")
		}
		if len(input.ExclusiveStartKey) == 0 {
			return &dynamodb.QueryOutput{Items: []attributes{db.items[first]}, LastEvaluatedKey: keyAttributes(first)}, nil
		}
		if keyOf(input.ExclusiveStartKey) != first {
			return nil, errors.New("query did not use the previous cursor")
		}
		return &dynamodb.QueryOutput{Items: []attributes{db.items[second]}}, nil
	}
	page, cursor, err := ledger.Executors(t.Context(), "dev", "build", lifecycle.Key{}, 1)
	if err != nil || len(page) != 1 || page[0].Binding.AllocationID != "a" || cursor != first {
		t.Fatalf("first allocation page = %+v, cursor = %+v, error = %v", page, cursor, err)
	}
	page, cursor, err = ledger.Executors(t.Context(), "dev", "build", cursor, 1)
	if err != nil || len(page) != 1 || page[0].Binding.AllocationID != "b" || cursor != (lifecycle.Key{}) {
		t.Fatalf("last allocation page = %+v, cursor = %+v, error = %v", page, cursor, err)
	}
	if _, _, err := ledger.Executors(t.Context(), "dev", "build", lifecycle.ProfileKey("dev", "build", "RUNNER-ID#7"), 1); err == nil || calls != 2 {
		t.Fatalf("foreign cursor reached the database: calls = %d, error = %v", calls, err)
	}
}

func TestExecutorQueryRejectsForeignRowsAndCursors(t *testing.T) {
	for _, foreignCursor := range []bool{false, true} {
		ledger, db := store(t)
		foreign := lifecycle.ProfileKey("dev", "other", "RUNNER#owned")
		row := record("dev", foreign, `{}`)
		if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: row}); err != nil {
			t.Fatal(err)
		}
		db.query = func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			if foreignCursor {
				return &dynamodb.QueryOutput{LastEvaluatedKey: keyAttributes(foreign)}, nil
			}
			return &dynamodb.QueryOutput{Items: []attributes{db.items[foreign]}}, nil
		}
		if _, _, err := ledger.Executors(t.Context(), "dev", "build", lifecycle.Key{}, 1); err == nil {
			t.Fatalf("foreign allocation query data accepted, cursor = %t", foreignCursor)
		}
	}
}
