package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Executors reads a bounded page of allocations from the base table. Runner ID
// lookups and effect history are outside the RUNNER# prefix.
func (s *Store) Executors(ctx context.Context, deployment, profile string, after Key, limit int32) ([]Executor, Key, error) {
	if deployment == "" || profile == "" || limit <= 0 {
		return nil, Key{}, errors.New("executor query requires deployment, profile and a positive limit")
	}
	partition := ProfileKey(deployment, profile, "").PK
	if after != (Key{}) && (after.PK != partition || !strings.HasPrefix(after.SK, "RUNNER#")) {
		return nil, Key{}, errors.New("executor cursor belongs to another query")
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(limit), ScanIndexForward: aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(partition), ":prefix": text("RUNNER#"),
		},
	}
	if after != (Key{}) {
		input.ExclusiveStartKey = after.item()
	}
	page, err := s.db.Query(ctx, input)
	if err != nil {
		return nil, Key{}, fmt.Errorf("query executor allocations: %w", err)
	}
	executors := make([]Executor, 0, len(page.Items))
	for _, item := range page.Items {
		record, err := readRecord(item)
		if err != nil {
			return nil, Key{}, err
		}
		allocation, matches := strings.CutPrefix(record.Key.SK, "RUNNER#")
		if record.Deployment != deployment || record.Key.PK != partition || !matches || allocation == "" {
			return nil, Key{}, errors.New("executor query returned a foreign allocation")
		}
		executor, err := decodeExecutor(record, allocation)
		if err != nil {
			return nil, Key{}, err
		}
		executors = append(executors, executor)
	}
	cursor := readKey(page.LastEvaluatedKey)
	if cursor != (Key{}) && (cursor.PK != partition || !strings.HasPrefix(cursor.SK, "RUNNER#")) {
		return nil, Key{}, errors.New("executor query returned a foreign cursor")
	}
	return executors, cursor, nil
}
