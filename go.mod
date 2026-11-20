module github.com/knightfall/scaleset-example-aws

go 1.27.4

ignore ./cdk

require (
	github.com/actions/scaleset v0.5.0
	github.com/aws/aws-sdk-go-v2/config v1.33.21
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.82.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.74.1
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.349.0
	github.com/aws/aws-sdk-go-v2/service/ecs v1.106.0
	github.com/aws/aws-sdk-go-v2/service/iam v1.68.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.116.0
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.51.0
	github.com/aws/aws-sdk-go-v2/service/sqs v1.53.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.53.0
)
