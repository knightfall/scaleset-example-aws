package sdkcontract

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

func awsConfig(wire *scriptedHTTP) aws.Config {
	return aws.Config{
		Region: "ap-southeast-2",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fixture-access-key", SecretAccessKey: "fixture-secret-key"}, nil
		}),
		HTTPClient: wire,
	}
}

func checkForm(t *testing.T, r *http.Request, want map[string]string) {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := url.ParseQuery(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range want {
		checkValue(t, name, fields.Get(name), value)
	}
}

func TestASGInventoryAndCapacityWrites(t *testing.T) {
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200,
			response: `<DescribeAutoScalingGroupsResponse>
				<DescribeAutoScalingGroupsResult><AutoScalingGroups><member>
					<AutoScalingGroupName>runners</AutoScalingGroupName><MinSize>1</MinSize><DesiredCapacity>2</DesiredCapacity><MaxSize>5</MaxSize>
					<Instances><member><InstanceId>i-fixture</InstanceId><LifecycleState>InService</LifecycleState><ProtectedFromScaleIn>true</ProtectedFromScaleIn><WeightedCapacity>2</WeightedCapacity></member></Instances>
				</member></AutoScalingGroups><NextToken>inventory-page-2</NextToken></DescribeAutoScalingGroupsResult>
			</DescribeAutoScalingGroupsResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "DescribeAutoScalingGroups", "AutoScalingGroupNames.member.1": "runners"})
			}},
		exchange{method: "POST", path: "/", status: 503,
			response: `<ErrorResponse><Error><Code>ServiceUnavailable</Code><Message>fixture: ambiguous write</Message></Error></ErrorResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "SetDesiredCapacity", "AutoScalingGroupName": "runners", "DesiredCapacity": "4", "HonorCooldown": "true"})
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `<TerminateInstanceInAutoScalingGroupResponse><TerminateInstanceInAutoScalingGroupResult><Activity><ActivityId>activity-1</ActivityId><StatusCode>InProgress</StatusCode></Activity></TerminateInstanceInAutoScalingGroupResult></TerminateInstanceInAutoScalingGroupResponse>`,
			check: func(r *http.Request) {
				checkForm(t, r, map[string]string{"Action": "TerminateInstanceInAutoScalingGroup", "InstanceId": "i-fixture", "ShouldDecrementDesiredCapacity": "true"})
			}},
	)
	client := autoscaling.NewFromConfig(awsConfig(wire))
	inventory, err := client.DescribeAutoScalingGroups(t.Context(), &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"runners"},
	})
	if err != nil || inventory == nil || len(inventory.AutoScalingGroups) != 1 || aws.ToString(inventory.NextToken) != "inventory-page-2" {
		t.Fatalf("inventory = %+v, error = %v", inventory, err)
	}
	group := inventory.AutoScalingGroups[0]
	if aws.ToInt32(group.DesiredCapacity) != 2 || aws.ToInt32(group.MinSize) != 1 ||
		len(group.Instances) != 1 || !aws.ToBool(group.Instances[0].ProtectedFromScaleIn) ||
		aws.ToString(group.Instances[0].WeightedCapacity) != "2" {
		t.Fatalf("unexpected group capacity/protection: %+v", group)
	}
	oneAttempt := func(options *autoscaling.Options) { options.Retryer = aws.NopRetryer{} }
	_, err = client.SetDesiredCapacity(t.Context(), &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String("runners"), DesiredCapacity: aws.Int32(4), HonorCooldown: aws.Bool(true),
	}, oneAttempt)
	if err == nil {
		t.Fatal("expected ambiguous desired-capacity write failure")
	}
	// A separate fixture covers targeted cleanup. It does not imply that a controller
	// may proceed to cleanup while an earlier write is still unresolved.
	result, err := client.TerminateInstanceInAutoScalingGroup(t.Context(), &autoscaling.TerminateInstanceInAutoScalingGroupInput{
		InstanceId: aws.String("i-fixture"), ShouldDecrementDesiredCapacity: aws.Bool(true),
	}, oneAttempt)
	if err != nil || result == nil || result.Activity == nil || aws.ToString(result.Activity.ActivityId) != "activity-1" {
		t.Fatalf("termination result = %+v, error = %v", result, err)
	}
}

func TestASGPreflightReads(t *testing.T) {
	checkAction := func(action string) func(*http.Request) {
		return func(r *http.Request) {
			checkForm(t, r, map[string]string{"Action": action, "AutoScalingGroupName": "runners"})
		}
	}
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200, check: checkAction("DescribeScalingActivities"),
			response: `<DescribeScalingActivitiesResponse><DescribeScalingActivitiesResult><Activities><member><ActivityId>activity-1</ActivityId><StatusCode>InProgress</StatusCode></member></Activities></DescribeScalingActivitiesResult></DescribeScalingActivitiesResponse>`},
		exchange{method: "POST", path: "/", status: 200, check: checkAction("DescribePolicies"),
			response: `<DescribePoliciesResponse><DescribePoliciesResult><ScalingPolicies><member><PolicyName>legacy-policy</PolicyName><PolicyType>TargetTrackingScaling</PolicyType></member></ScalingPolicies></DescribePoliciesResult></DescribePoliciesResponse>`},
		exchange{method: "POST", path: "/", status: 200, check: checkAction("DescribeScheduledActions"),
			response: `<DescribeScheduledActionsResponse><DescribeScheduledActionsResult><ScheduledUpdateGroupActions><member><ScheduledActionName>legacy-schedule</ScheduledActionName><DesiredCapacity>6</DesiredCapacity></member></ScheduledUpdateGroupActions></DescribeScheduledActionsResult></DescribeScheduledActionsResponse>`},
	)
	client := autoscaling.NewFromConfig(awsConfig(wire))
	name := aws.String("runners")
	activities, err := client.DescribeScalingActivities(t.Context(), &autoscaling.DescribeScalingActivitiesInput{AutoScalingGroupName: name})
	if err != nil || activities == nil || len(activities.Activities) != 1 || activities.Activities[0].StatusCode != "InProgress" {
		t.Fatalf("scaling activities = %+v, error = %v", activities, err)
	}
	policies, err := client.DescribePolicies(t.Context(), &autoscaling.DescribePoliciesInput{AutoScalingGroupName: name})
	if err != nil || policies == nil || len(policies.ScalingPolicies) != 1 || aws.ToString(policies.ScalingPolicies[0].PolicyType) != "TargetTrackingScaling" {
		t.Fatalf("scaling policies = %+v, error = %v", policies, err)
	}
	schedule, err := client.DescribeScheduledActions(t.Context(), &autoscaling.DescribeScheduledActionsInput{AutoScalingGroupName: name})
	if err != nil || schedule == nil || len(schedule.ScheduledUpdateGroupActions) != 1 || aws.ToInt32(schedule.ScheduledUpdateGroupActions[0].DesiredCapacity) != 6 {
		t.Fatalf("scheduled actions = %+v, error = %v", schedule, err)
	}
}

func TestECSLaunchRecoveryAndStop(t *testing.T) {
	const cluster = "arn:aws:ecs:ap-southeast-2:111122223333:cluster/runners"
	const definition = "arn:aws:ecs:ap-southeast-2:111122223333:task-definition/linux-x64:7"
	const task = "arn:aws:ecs:ap-southeast-2:111122223333:task/runners/task-fixture"
	const token = "allocation-fixture"
	const runBody = `{
		"cluster":"` + cluster + `", "taskDefinition":"` + definition + `", "count":1,
		"clientToken":"` + token + `", "startedBy":"` + token + `", "launchType":"FARGATE",
		"networkConfiguration":{"awsvpcConfiguration":{"subnets":["subnet-fixture"],"securityGroups":["sg-fixture"],"assignPublicIp":"DISABLED"}},
		"tags":[{"key":"owner","value":"controller"}]
	}`
	launch := exchange{method: "POST", path: "/", status: 200,
		response: `{"tasks":[{"taskArn":"` + task + `","lastStatus":"PROVISIONING"}],"failures":[{"reason":"fixture-failure"}]}`,
		check: func(r *http.Request) {
			checkValue(t, "target", r.Header.Get("X-Amz-Target"), "AmazonEC2ContainerServiceV20141113.RunTask")
			checkJSON(t, r, runBody)
		}}
	wire := newScript(t, launch, launch,
		exchange{method: "POST", path: "/", status: 200, response: `{"taskArns":["` + task + `"]}`,
			check: func(r *http.Request) {
				checkValue(t, "target", r.Header.Get("X-Amz-Target"), "AmazonEC2ContainerServiceV20141113.ListTasks")
				checkJSON(t, r, `{"cluster":"`+cluster+`","startedBy":"`+token+`"}`)
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `{"tasks":[{"taskArn":"` + task + `","lastStatus":"STOPPED","containers":[{"name":"runner","exitCode":0}]}]}`,
			check: func(r *http.Request) {
				checkJSON(t, r, `{"cluster":"`+cluster+`","tasks":["`+task+`"],"include":["TAGS"]}`)
			}},
		exchange{method: "POST", path: "/", status: 200,
			response: `{"task":{"taskArn":"` + task + `","desiredStatus":"STOPPED"}}`,
			check: func(r *http.Request) {
				checkValue(t, "target", r.Header.Get("X-Amz-Target"), "AmazonEC2ContainerServiceV20141113.StopTask")
				checkJSON(t, r, `{"cluster":"`+cluster+`","task":"`+task+`","reason":"fixture completed"}`)
			}},
	)
	client := ecs.NewFromConfig(awsConfig(wire))
	input := &ecs.RunTaskInput{
		Cluster: aws.String(cluster), TaskDefinition: aws.String(definition), Count: aws.Int32(1),
		ClientToken: aws.String(token), StartedBy: aws.String(token), LaunchType: ecstypes.LaunchTypeFargate,
		NetworkConfiguration: &ecstypes.NetworkConfiguration{
			AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
				Subnets: []string{"subnet-fixture"}, SecurityGroups: []string{"sg-fixture"}, AssignPublicIp: ecstypes.AssignPublicIpDisabled,
			},
		},
		Tags: []ecstypes.Tag{{Key: aws.String("owner"), Value: aws.String("controller")}},
	}
	for range 2 {
		result, err := client.RunTask(t.Context(), input)
		if err != nil || result == nil || len(result.Tasks) != 1 || aws.ToString(result.Tasks[0].TaskArn) != task ||
			len(result.Failures) != 1 || aws.ToString(result.Failures[0].Reason) != "fixture-failure" {
			t.Fatalf("launch result = %+v, error = %v", result, err)
		}
	}
	list, err := client.ListTasks(t.Context(), &ecs.ListTasksInput{Cluster: aws.String(cluster), StartedBy: aws.String(token)})
	if err != nil || list == nil || len(list.TaskArns) != 1 || list.TaskArns[0] != task {
		t.Fatalf("list result = %+v, error = %v", list, err)
	}
	read, err := client.DescribeTasks(t.Context(), &ecs.DescribeTasksInput{
		Cluster: aws.String(cluster), Tasks: []string{task}, Include: []ecstypes.TaskField{ecstypes.TaskFieldTags},
	})
	if err != nil || read == nil || len(read.Tasks) != 1 || aws.ToString(read.Tasks[0].LastStatus) != "STOPPED" ||
		len(read.Tasks[0].Containers) != 1 || read.Tasks[0].Containers[0].ExitCode == nil || *read.Tasks[0].Containers[0].ExitCode != 0 {
		t.Fatalf("task result = %+v, error = %v", read, err)
	}
	stop, err := client.StopTask(t.Context(), &ecs.StopTaskInput{
		Cluster: aws.String(cluster), Task: aws.String(task), Reason: aws.String("fixture completed"),
	})
	if err != nil || stop == nil || stop.Task == nil || aws.ToString(stop.Task.DesiredStatus) != "STOPPED" {
		t.Fatalf("stop result = %+v, error = %v", stop, err)
	}
}

func TestDynamoDBConditionalHandoff(t *testing.T) {
	const consumeBody = `{
		"TableName":"lifecycle", "Key":{"PK":{"S":"OFFER#fixture"},"SK":{"S":"BOOTSTRAP"}},
		"ConditionExpression":"bootNonce = :nonce", "ExpressionAttributeValues":{":nonce":{"S":"boot-fixture"}},
		"ReturnValues":"ALL_OLD"
	}`
	consume := exchange{method: "POST", path: "/", status: 200,
		response: `{"Attributes":{"ciphertext":{"B":"AQID"}}}`,
		check: func(r *http.Request) {
			checkValue(t, "target", r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.DeleteItem")
			checkJSON(t, r, consumeBody)
		}}
	duplicate := consume
	duplicate.status = 400
	duplicate.response = `{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"already consumed"}`
	wire := newScript(t,
		exchange{method: "POST", path: "/", status: 200, response: `{}`,
			check: func(r *http.Request) {
				checkValue(t, "target", r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.TransactWriteItems")
				checkJSON(t, r, `{
					"ClientRequestToken":"offer-fixture",
					"TransactItems":[
						{"ConditionCheck":{"TableName":"lifecycle","Key":{"PK":{"S":"ENV#fixture"},"SK":{"S":"LEASE"}},"ConditionExpression":"epoch = :epoch","ExpressionAttributeValues":{":epoch":{"N":"4"}}}},
						{"Put":{"TableName":"lifecycle","Item":{"PK":{"S":"OFFER#fixture"},"SK":{"S":"BOOTSTRAP"},"bootNonce":{"S":"boot-fixture"},"ciphertext":{"B":"AQID"}},"ConditionExpression":"attribute_not_exists(PK)"}}
					]
				}`)
			}},
		consume,
		duplicate,
	)
	client := dynamodb.NewFromConfig(awsConfig(wire))
	itemKey := func(pk, sk string) map[string]ddbtypes.AttributeValue {
		return map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		}
	}
	offer := itemKey("OFFER#fixture", "BOOTSTRAP")
	offer["bootNonce"] = &ddbtypes.AttributeValueMemberS{Value: "boot-fixture"}
	offer["ciphertext"] = &ddbtypes.AttributeValueMemberB{Value: []byte{1, 2, 3}}
	_, err := client.TransactWriteItems(t.Context(), &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String("offer-fixture"), TransactItems: []ddbtypes.TransactWriteItem{
			{ConditionCheck: &ddbtypes.ConditionCheck{
				TableName: aws.String("lifecycle"), Key: itemKey("ENV#fixture", "LEASE"), ConditionExpression: aws.String("epoch = :epoch"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":epoch": &ddbtypes.AttributeValueMemberN{Value: "4"}},
			}},
			{Put: &ddbtypes.Put{TableName: aws.String("lifecycle"), Item: offer, ConditionExpression: aws.String("attribute_not_exists(PK)")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String("lifecycle"), Key: itemKey("OFFER#fixture", "BOOTSTRAP"), ConditionExpression: aws.String("bootNonce = :nonce"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":nonce": &ddbtypes.AttributeValueMemberS{Value: "boot-fixture"}},
		ReturnValues:              ddbtypes.ReturnValueAllOld,
	}
	result, err := client.DeleteItem(t.Context(), input)
	if err != nil || result == nil {
		t.Fatalf("consume result = %+v, error = %v", result, err)
	}
	if ciphertext, ok := result.Attributes["ciphertext"].(*ddbtypes.AttributeValueMemberB); !ok || len(ciphertext.Value) != 3 {
		t.Fatalf("unexpected ciphertext shape: %+v", result.Attributes["ciphertext"])
	}
	_, err = client.DeleteItem(t.Context(), input)
	if _, ok := errors.AsType[*ddbtypes.ConditionalCheckFailedException](err); !ok {
		t.Fatalf("duplicate consume error = %v", err)
	}
}
