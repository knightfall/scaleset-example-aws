package githubsets

import (
	"context"
	"errors"

	"github.com/actions/scaleset"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

type Session struct {
	client            *scaleset.MessageSessionClient
	ownership         lifecycle.SessionOwnership
	deployment        string
	profile           string
	credentialVersion string
}

// AdaptSession wraps an already owned SDK session and exposes sanitized facts.
func AdaptSession(client *scaleset.MessageSessionClient) (*Session, error) {
	if client == nil || client.Session().SessionID.String() == "00000000-0000-0000-0000-000000000000" {
		return nil, errors.New("GitHub session requires a remote identity")
	}
	return &Session{client: client}, nil
}

func (s *Session) ID() string {
	return s.client.Session().SessionID.String()
}

func (s *Session) InitialDemand() *lifecycle.DemandCounts {
	return demandCounts(s.client.Session().Statistics)
}

func (s *Session) Poll(ctx context.Context, cursor int64, capacity int) (*lifecycle.Delivery, error) {
	if cursor < 0 || int64(int(cursor)) != cursor || capacity < 0 {
		return nil, errors.New("invalid GitHub message cursor or capacity")
	}
	ctx, status := trackResponseStatus(ctx)
	message, err := s.client.GetMessage(ctx, int(cursor), capacity)
	if err != nil {
		return nil, requestError("poll GitHub scale set", status.Load(), err)
	}
	if message == nil {
		return nil, nil
	}
	return translateDelivery(message)
}

func (s *Session) Acquire(ctx context.Context, ids []int64) ([]int64, error) {
	ctx, status := trackResponseStatus(ctx)
	ctx = oneWriteAttempt(ctx)
	accepted, err := s.client.AcquireJobs(ctx, ids)
	if err != nil {
		return nil, requestError("acquire GitHub jobs", status.Load(), err)
	}
	return accepted, nil
}

func (s *Session) Acknowledge(ctx context.Context, messageID int64) error {
	if messageID < 0 || int64(int(messageID)) != messageID {
		return errors.New("invalid GitHub message ID")
	}
	ctx, status := trackResponseStatus(ctx)
	if err := s.client.DeleteMessage(oneWriteAttempt(ctx), int(messageID)); err != nil {
		return requestError("acknowledge GitHub delivery", status.Load(), err)
	}
	return nil
}

func oneWriteAttempt(ctx context.Context) context.Context {
	return scaleset.WithRequestRetry(ctx, func(retry *scaleset.RetryConfig) {
		retry.Max = 0
	})
}

func translateDelivery(message *scaleset.RunnerScaleSetMessage) (*lifecycle.Delivery, error) {
	delivery := &lifecycle.Delivery{MessageID: int64(message.MessageID), Stats: demandCounts(message.Statistics)}
	for _, offer := range message.JobAvailableMessages {
		if offer == nil {
			return nil, errors.New("GitHub delivery contains an empty job offer")
		}
		delivery.Offers = append(delivery.Offers, offer.RunnerRequestID)
	}
	for _, job := range message.JobAssignedMessages {
		if job == nil {
			return nil, errors.New("GitHub delivery contains an empty assignment")
		}
		delivery.Jobs = append(delivery.Jobs, lifecycle.JobFact{
			RequestID: job.RunnerRequestID, Phase: lifecycle.JobAssigned, OccurredAt: job.ScaleSetAssignTime.UTC(),
		})
	}
	for _, job := range message.JobStartedMessages {
		if job == nil {
			return nil, errors.New("GitHub delivery contains an empty job start")
		}
		delivery.Jobs = append(delivery.Jobs, lifecycle.JobFact{
			RequestID: job.RunnerRequestID, Phase: lifecycle.JobStarted,
			RunnerID: job.RunnerID, OccurredAt: job.RunnerAssignTime.UTC(),
		})
	}
	for _, job := range message.JobCompletedMessages {
		if job == nil {
			return nil, errors.New("GitHub delivery contains an empty completion")
		}
		delivery.Jobs = append(delivery.Jobs, lifecycle.JobFact{
			RequestID: job.RunnerRequestID, Phase: lifecycle.JobCompleted,
			RunnerID: job.RunnerID, OccurredAt: job.FinishTime.UTC(),
		})
	}
	return delivery, nil
}

func demandCounts(statistics *scaleset.RunnerScaleSetStatistic) *lifecycle.DemandCounts {
	if statistics == nil {
		return nil
	}
	return &lifecycle.DemandCounts{
		Assigned: statistics.TotalAssignedJobs, Running: statistics.TotalRunningJobs,
		Registered: statistics.TotalRegisteredRunners, Busy: statistics.TotalBusyRunners,
		Idle: statistics.TotalIdleRunners, Acquired: statistics.TotalAcquiredJobs,
		Available: statistics.TotalAvailableJobs,
	}
}
