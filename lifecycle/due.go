package lifecycle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func duePartitionKey(deployment string, shard int) string {
	return "ENV#" + deployment + "#DUE#" + strconv.Itoa(shard)
}

// Due uses the sparse index to find candidates and confirms each one in the
// base table before returning it. Callers still need a conditional transition.
func (s *Store) Due(ctx context.Context, deployment string, before time.Time, limit int32) ([]Record, error) {
	if deployment == "" || before.IsZero() || limit <= 0 {
		return nil, errors.New("due query requires deployment, deadline and a positive limit")
	}
	var records []Record
	seen := make(map[Key]bool)
	for shard := range dueShards {
		input := &dynamodb.QueryInput{
			TableName: aws.String(s.table), IndexName: aws.String(DueIndexName),
			Limit: aws.Int32(limit), ScanIndexForward: aws.Bool(true),
			KeyConditionExpression:   aws.String("#shard = :shard AND #due <= :before"),
			ExpressionAttributeNames: map[string]string{"#shard": DuePartition, "#due": DueSort},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":shard":  text(duePartitionKey(deployment, shard)),
				":before": number(before.UnixMilli()),
			},
			ProjectionExpression: aws.String("PK, SK"),
		}
		var accepted int32
		for accepted < limit {
			page, err := s.db.Query(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("discover due lifecycle records: %w", err)
			}
			for _, item := range page.Items {
				key := readKey(item)
				if seen[key] {
					continue
				}
				seen[key] = true
				record, err := s.Get(ctx, key)
				if err != nil {
					return nil, err
				}
				if record == nil || record.Deployment != deployment || record.DueAt.IsZero() || record.DueAt.After(before) {
					continue
				}
				records = append(records, *record)
				accepted++
				if accepted == limit {
					break
				}
			}
			if len(page.LastEvaluatedKey) == 0 {
				break
			}
			input.ExclusiveStartKey = page.LastEvaluatedKey
		}
	}
	slices.SortFunc(records, func(a, b Record) int {
		if order := a.DueAt.Compare(b.DueAt); order != 0 {
			return order
		}
		if order := cmp.Compare(a.Key.PK, b.Key.PK); order != 0 {
			return order
		}
		return cmp.Compare(a.Key.SK, b.Key.SK)
	})
	if len(records) > int(limit) {
		records = records[:limit]
	}
	return records, nil
}
