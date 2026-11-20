package sdkcontract

import (
	"io"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func TestAWSConfigAndCredentialReads(t *testing.T) {
	const secretARN = "arn:aws:secretsmanager:ap-southeast-2:111122223333:secret:github-fixture"
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200,
			response: `{"ARN":"` + secretARN + `","VersionId":"secret-version-fixture","SecretString":"{\"token\":\"fixture\"}"}`,
			check: func(r *http.Request) {
				checkJSON(t, r, `{"SecretId":"`+secretARN+`","VersionStage":"AWSCURRENT"}`)
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `<GetCallerIdentityResponse><GetCallerIdentityResult><UserId>AROFIXTURE:i-fixture</UserId><Account>111122223333</Account><Arn>arn:aws:sts::111122223333:assumed-role/runner/i-fixture</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>`,
			check:    func(r *http.Request) { checkForm(t, r, map[string]string{"Action": "GetCallerIdentity"}) }},
		exchange{method: "POST", path: "/", status: 200,
			response: `<GetRoleResponse><GetRoleResult><Role><RoleName>runner</RoleName><RoleId>AROFIXTURE</RoleId><Arn>arn:aws:iam::111122223333:role/runner</Arn></Role></GetRoleResult></GetRoleResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "GetRole", "RoleName": "runner"})
			}},
	)
	fixture := awsConfig(wire)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	home := t.TempDir()
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithSharedConfigFiles([]string{filepath.Join(home, "config")}),
		config.WithSharedCredentialsFiles([]string{filepath.Join(home, "credentials")}),
		config.WithRegion(fixture.Region),
		config.WithCredentialsProvider(fixture.Credentials),
		config.WithHTTPClient(fixture.HTTPClient),
		config.WithDefaultsMode(aws.DefaultsModeStandard),
	)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := secretsmanager.NewFromConfig(cfg).GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretARN), VersionStage: aws.String("AWSCURRENT"),
	})
	if err != nil || secret == nil || aws.ToString(secret.VersionId) != "secret-version-fixture" ||
		aws.ToString(secret.SecretString) != `{"token":"fixture"}` {
		t.Fatalf("unexpected secret fixture response: %v", err)
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
	if err != nil || identity == nil || aws.ToString(identity.UserId) != "AROFIXTURE:i-fixture" {
		t.Fatalf("identity = %+v, error = %v", identity, err)
	}
	role, err := iam.NewFromConfig(cfg).GetRole(t.Context(), &iam.GetRoleInput{RoleName: aws.String("runner")})
	if err != nil || role == nil || role.Role == nil || aws.ToString(role.Role.RoleId) != "AROFIXTURE" {
		t.Fatalf("role = %+v, error = %v", role, err)
	}
}

func TestEC2Inventory(t *testing.T) {
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200,
			response: `<DescribeInstancesResponse><reservationSet><item><instancesSet><item>
				<instanceId>i-fixture</instanceId><architecture>arm64</architecture>
				<iamInstanceProfile><arn>arn:aws:iam::111122223333:instance-profile/runner</arn></iamInstanceProfile>
				<metadataOptions><httpTokens>required</httpTokens></metadataOptions>
			</item></instancesSet></item></reservationSet></DescribeInstancesResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "DescribeInstances", "InstanceId.1": "i-fixture"})
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `<DescribeLaunchTemplateVersionsResponse><launchTemplateVersionSet><item>
				<launchTemplateId>lt-fixture</launchTemplateId><versionNumber>7</versionNumber>
				<launchTemplateData><imageId>ami-fixture</imageId></launchTemplateData>
			</item></launchTemplateVersionSet></DescribeLaunchTemplateVersionsResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "DescribeLaunchTemplateVersions", "LaunchTemplateId": "lt-fixture", "LaunchTemplateVersion.1": "7"})
			}},
	)
	client := ec2.NewFromConfig(awsConfig(wire))
	instances, err := client.DescribeInstances(t.Context(), &ec2.DescribeInstancesInput{InstanceIds: []string{"i-fixture"}})
	if err != nil || instances == nil || len(instances.Reservations) != 1 || len(instances.Reservations[0].Instances) != 1 {
		t.Fatalf("instances = %+v, error = %v", instances, err)
	}
	instance := instances.Reservations[0].Instances[0]
	if aws.ToString(instance.InstanceId) != "i-fixture" || instance.Architecture != "arm64" ||
		instance.MetadataOptions == nil || instance.MetadataOptions.HttpTokens != "required" ||
		instance.IamInstanceProfile == nil || aws.ToString(instance.IamInstanceProfile.Arn) != "arn:aws:iam::111122223333:instance-profile/runner" {
		t.Fatalf("instance provenance = %+v", instance)
	}
	versions, err := client.DescribeLaunchTemplateVersions(t.Context(), &ec2.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateId: aws.String("lt-fixture"), Versions: []string{"7"},
	})
	if err != nil || versions == nil || len(versions.LaunchTemplateVersions) != 1 {
		t.Fatalf("launch templates = %+v, error = %v", versions, err)
	}
	version := versions.LaunchTemplateVersions[0]
	if aws.ToInt64(version.VersionNumber) != 7 || version.LaunchTemplateData == nil ||
		aws.ToString(version.LaunchTemplateData.ImageId) != "ami-fixture" {
		t.Fatalf("unexpected launch template revision: %+v", version)
	}
}

func TestECSPreflight(t *testing.T) {
	const revision = "arn:aws:ecs:ap-southeast-2:111122223333:task-definition/runner:7"
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200, response: `{"clusters":[{"clusterName":"runners","status":"ACTIVE"}]}`,
			check: func(r *http.Request) { checkJSON(t, r, `{"clusters":["runners"]}`) }},
		exchange{method: "POST", path: "/", status: 200,
			response: `{"taskDefinition":{"taskDefinitionArn":"` + revision + `","revision":7,"status":"ACTIVE","requiresCompatibilities":["FARGATE"],"networkMode":"awsvpc","runtimePlatform":{"cpuArchitecture":"ARM64","operatingSystemFamily":"LINUX"},"containerDefinitions":[{"name":"runner","essential":true}]}}`,
			check:    func(r *http.Request) { checkJSON(t, r, `{"taskDefinition":"`+revision+`"}`) }},
	)
	client := ecs.NewFromConfig(awsConfig(wire))
	clusters, err := client.DescribeClusters(t.Context(), &ecs.DescribeClustersInput{Clusters: []string{"runners"}})
	if err != nil || clusters == nil || len(clusters.Clusters) != 1 || aws.ToString(clusters.Clusters[0].Status) != "ACTIVE" {
		t.Fatalf("clusters = %+v, error = %v", clusters, err)
	}
	result, err := client.DescribeTaskDefinition(t.Context(), &ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String(revision)})
	if err != nil || result == nil || result.TaskDefinition == nil {
		t.Fatalf("task definition = %+v, error = %v", result, err)
	}
	definition := result.TaskDefinition
	if aws.ToString(definition.TaskDefinitionArn) != revision || definition.NetworkMode != ecstypes.NetworkModeAwsvpc ||
		len(definition.RequiresCompatibilities) != 1 || definition.RequiresCompatibilities[0] != ecstypes.CompatibilityFargate ||
		definition.RuntimePlatform == nil || definition.RuntimePlatform.CpuArchitecture != ecstypes.CPUArchitectureArm64 ||
		len(definition.ContainerDefinitions) != 1 || !aws.ToBool(definition.ContainerDefinitions[0].Essential) {
		t.Fatalf("unexpected task definition: %+v", definition)
	}
}

func TestConfigurationObjectAndQueueReceipt(t *testing.T) {
	const queueURL = "https://sqs.ap-southeast-2.amazonaws.com/111122223333/lifecycle"
	wire := newScript(t,
		exchange{method: "GET", path: "/config/plan.json", status: 200, response: `{"mode":"dry-run"}`,
			check: func(r *http.Request) {
				checkValue(t, "S3 bucket endpoint", r.URL.Host, "fixture-config.s3.ap-southeast-2.amazonaws.com")
				checkValue(t, "S3 version ID", r.URL.Query().Get("versionId"), "fixture-version")
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `{"Messages":[{"MessageId":"event-1","ReceiptHandle":"receipt-fixture","Body":"event-body"}]}`,
			check: func(r *http.Request) {
				checkValue(t, "target", r.Header.Get("X-Amz-Target"), "AmazonSQS.ReceiveMessage")
				checkJSON(t, r, `{"QueueUrl":"`+queueURL+`","WaitTimeSeconds":20}`)
			}},
		exchange{method: "POST", path: "/", status: 200, response: `{}`,
			check: func(r *http.Request) {
				checkValue(t, "target", r.Header.Get("X-Amz-Target"), "AmazonSQS.DeleteMessage")
				checkJSON(t, r, `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"receipt-fixture"}`)
			}},
	)
	cfg := awsConfig(wire)
	object, err := s3.NewFromConfig(cfg).GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("fixture-config"), Key: aws.String("config/plan.json"), VersionId: aws.String("fixture-version"),
	})
	if err != nil || object == nil || object.Body == nil {
		t.Fatalf("object = %+v, error = %v", object, err)
	}
	body, err := io.ReadAll(object.Body)
	if closeErr := object.Body.Close(); closeErr != nil {
		t.Errorf("close object: %v", closeErr)
	}
	if err != nil || string(body) != `{"mode":"dry-run"}` {
		t.Fatalf("object = %q, error = %v", body, err)
	}
	client := sqs.NewFromConfig(cfg)
	result, err := client.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), WaitTimeSeconds: 20})
	if err != nil || result == nil || len(result.Messages) != 1 || aws.ToString(result.Messages[0].MessageId) != "event-1" {
		t.Fatalf("queue events = %+v, error = %v", result, err)
	}
	if _, err := client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
		QueueUrl: aws.String(queueURL), ReceiptHandle: result.Messages[0].ReceiptHandle,
	}); err != nil {
		t.Fatal(err)
	}
}
