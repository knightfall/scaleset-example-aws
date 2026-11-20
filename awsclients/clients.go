// Package awsclients creates AWS runner clients only for the selected backends.
package awsclients

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecs"

	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type ASGClients struct {
	Scaling   *autoscaling.Client
	Inventory *ec2.Client
}

type Clients struct {
	ASG *ASGClients
	ECS *ecs.Client
}

// Constructors uses the SDK by default. Callers can supply different construction
// functions for credential or endpoint isolation.
type Constructors struct {
	ASG     func(aws.Config) *ASGClients
	Fargate func(aws.Config) *ecs.Client
}

// Load validates the artifact before constructing any AWS service clients.
func (c Constructors) Load(
	decoder *runtimeconfig.Decoder, data []byte, expectedSHA256 string, awsConfig aws.Config,
) (runtimeconfig.Artifact, Clients, error) {
	artifact, err := decoder.Decode(data, expectedSHA256)
	var clients Clients
	if err != nil {
		return artifact, clients, err
	}
	spec := artifact.Config.Spec
	if spec.Mode == "dry-run" && !spec.DryRun.InspectAWS {
		return artifact, clients, nil
	}

	awsConfig.Region = spec.AWS.Region
	for _, kind := range artifact.EnabledProvisioners() {
		switch kind {
		case "asg":
			if c.ASG != nil {
				clients.ASG = c.ASG(awsConfig)
			} else {
				clients.ASG = &ASGClients{
					Scaling:   autoscaling.NewFromConfig(awsConfig),
					Inventory: ec2.NewFromConfig(awsConfig),
				}
			}
		case "fargate":
			if c.Fargate != nil {
				clients.ECS = c.Fargate(awsConfig)
			} else {
				clients.ECS = ecs.NewFromConfig(awsConfig)
			}
		}
	}
	return artifact, clients, nil
}
