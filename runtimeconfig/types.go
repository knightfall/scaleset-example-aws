// Package runtimeconfig loads and validates the controller's resolved configuration.
package runtimeconfig

// Artifact is the versioned runtime document produced by the CDK config compiler.
type Artifact struct {
	APIVersion    string      `json:"apiVersion"`
	Kind          string      `json:"kind"`
	Config        Environment `json:"config"`
	Resources     Resources   `json:"resources"`
	sourceDigest  string
	decodedDigest string
}

type Environment struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec Spec `json:"spec"`
}

type Spec struct {
	AWS        AWS        `json:"aws"`
	Mode       string     `json:"mode"`
	DryRun     DryRun     `json:"dryRun"`
	Controller Controller `json:"controller"`
	Tracking   struct {
		Table Table `json:"table"`
	} `json:"tracking"`
	GitHub      map[string]GitHubConnection `json:"github"`
	Profiles    []Profile                   `json:"profiles"`
	Networks    map[string]Network          `json:"networks"`
	ECSClusters map[string]Cluster          `json:"ecsClusters"`
}

type AWS struct {
	Account string `json:"account"`
	Region  string `json:"region"`
}

type DryRun struct {
	InspectAWS bool        `json:"inspectAws"`
	Demand     DemandInput `json:"demand"`
}

type DemandInput struct {
	Type string    `json:"type"`
	Path string    `json:"path"`
	S3   *S3Object `json:"s3"`
}

type S3Object struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	SHA256    string `json:"sha256"`
	VersionID string `json:"versionId"`
}

type Controller struct {
	Image   string  `json:"image"`
	Hosting Hosting `json:"hosting"`
}

type Hosting struct {
	Type            string            `json:"type"`
	Identity        *ExternalIdentity `json:"identity"`
	Network         string            `json:"network"`
	AMIID           string            `json:"amiId"`
	InstanceType    string            `json:"instanceType"`
	Cluster         string            `json:"cluster"`
	CPUArchitecture string            `json:"cpuArchitecture"`
	CPU             int               `json:"cpu"`
	MemoryMiB       int               `json:"memoryMiB"`
}

type ExternalIdentity struct {
	Type             string `json:"type"`
	PrincipalRoleARN string `json:"principalRoleArn"`
	AssumeRoleARN    string `json:"assumeRoleArn"`
	TrustAnchorARN   string `json:"trustAnchorArn"`
	ProfileARN       string `json:"profileArn"`
	RoleARN          string `json:"roleArn"`
}

type Table struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type Network struct {
	VPCID            string   `json:"vpcId"`
	Subnets          []Subnet `json:"subnets"`
	SecurityGroupIDs []string `json:"securityGroupIds"`
}

type Subnet struct {
	ID string `json:"id"`
	AZ string `json:"az"`
}

type Cluster struct {
	Type string `json:"type"`
	ARN  string `json:"arn"`
	Name string `json:"name"`
}

type GitHubConnection struct {
	ScopeURL     string     `json:"scopeURL"`
	CABundlePath string     `json:"caBundlePath"`
	Auth         GitHubAuth `json:"auth"`
}

type GitHubAuth struct {
	Type      string `json:"type"`
	SecretARN string `json:"secretArn"`
}

type Profile struct {
	ID          string        `json:"id"`
	State       string        `json:"state"`
	Purpose     string        `json:"purpose"`
	Platform    Platform      `json:"platform"`
	Capacity    Capacity      `json:"capacity"`
	GitHub      GitHubBinding `json:"github"`
	Provisioner Provisioner   `json:"provisioner"`
}

type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

type Capacity struct {
	IdleReserve           int `json:"idleReserve"`
	MaxRunners            int `json:"maxRunners"`
	StartupTimeoutSeconds int `json:"startupTimeoutSeconds"`
}

type GitHubBinding struct {
	Connection   string   `json:"connection"`
	RunnerGroup  string   `json:"runnerGroup"`
	ScaleSet     string   `json:"scaleSet"`
	ScaleSetID   int64    `json:"scaleSetId"`
	Labels       []string `json:"labels"`
	Registration string   `json:"registration"`
}

type Provisioner struct {
	Type              string        `json:"type"`
	GroupARN          string        `json:"groupArn"`
	CapacityUnits     CapacityUnits `json:"capacityUnits"`
	Cluster           string        `json:"cluster"`
	Network           string        `json:"network"`
	TaskDefinitionARN string        `json:"taskDefinitionArn"`
	RunnerContainer   string        `json:"runnerContainer"`
	BootstrapRoleARN  string        `json:"bootstrapRoleArn"`
	ExecutionRoleARN  string        `json:"executionRoleArn"`
	BootstrapProtocol string        `json:"bootstrapProtocol"`
}

type CapacityUnits struct {
	Type   string `json:"type"`
	Weight int    `json:"weight"`
}

type Resources struct {
	TrackingTableName string            `json:"trackingTableName"`
	ECSClusterARNs    map[string]string `json:"ecsClusterArns"`
}

// EnabledProvisioners lists the distinct backend types used by active or draining profiles.
func (a Artifact) EnabledProvisioners() []string {
	var kinds []string
	seen := make(map[string]bool)
	for _, profile := range a.Config.Spec.Profiles {
		kind := profile.Provisioner.Type
		if profile.State == "disabled" || seen[kind] {
			continue
		}
		seen[kind] = true
		kinds = append(kinds, kind)
	}
	return kinds
}
