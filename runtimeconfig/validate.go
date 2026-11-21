package runtimeconfig

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"strings"
)

func (a Artifact) validate() error {
	spec := a.Config.Spec
	tableName := spec.Tracking.Table.Name
	if spec.Tracking.Table.Type == "create" {
		tableName = a.Config.Metadata.Name + "-runner-lifecycle"
	}
	if a.Resources.TrackingTableName != tableName {
		return errors.New("resolved tracking table does not match configuration")
	}

	bindings := deploymentBindings{
		spec:            spec,
		clusters:        make(map[string]string),
		clusterOwners:   make(map[string]bool),
		subnets:         make(map[string]subnetMapping),
		groupVPCs:       make(map[string]string),
		asgOwners:       make(map[string]bool),
		taskDefinitions: make(map[string]trustBoundary),
	}
	controllerRoles, err := bindings.checkHost()
	if err != nil {
		return err
	}
	scopes, err := checkGitHubProfiles(spec)
	if err != nil {
		return err
	}
	for _, profile := range spec.Profiles {
		if profile.State == "disabled" {
			continue
		}
		if err := bindings.checkExecution(profile, scopes[profile.GitHub.Connection], controllerRoles); err != nil {
			return err
		}
	}
	if !maps.Equal(bindings.clusters, a.Resources.ECSClusterARNs) {
		return errors.New("resolved ECS clusters do not match selected configuration")
	}
	return nil
}

func checkGitHubProfiles(spec Spec) (map[string]string, error) {
	scopes := make(map[string]string, len(spec.GitHub))
	for name, connection := range spec.GitHub {
		scope, err := connection.CanonicalScope()
		if err != nil {
			return nil, fmt.Errorf("GitHub connection %s: %w", name, err)
		}
		scopes[name] = scope
	}

	type setName struct{ scope, group, name string }
	type setID struct {
		scope string
		id    int64
	}
	type selector struct{ scope, label string }
	profileIDs := make(map[string]bool)
	setNames := make(map[setName]bool)
	setIDs := make(map[setID]bool)
	selectors := make(map[selector]bool)
	for _, profile := range spec.Profiles {
		if profileIDs[profile.ID] {
			return nil, fmt.Errorf("conflicting profile ID: %s", profile.ID)
		}
		profileIDs[profile.ID] = true
		if err := profile.validate(); err != nil {
			return nil, fmt.Errorf("profile %s: %w", profile.ID, err)
		}
		if profile.State == "disabled" {
			continue
		}

		github := profile.GitHub
		scope, ok := scopes[github.Connection]
		if !ok {
			return nil, fmt.Errorf("profile %s references a missing GitHub connection", profile.ID)
		}
		if err := spec.AWS.checkARN(spec.GitHub[github.Connection].Auth.SecretARN, "GitHub secret ARN"); err != nil {
			return nil, err
		}
		group := strings.ToLower(github.RunnerGroup)
		name := setName{scope, group, strings.ToLower(github.ScaleSet)}
		if setNames[name] {
			return nil, fmt.Errorf("conflicting scale set mapping for profile %s", profile.ID)
		}
		setNames[name] = true
		if github.ScaleSetID != 0 {
			id := setID{scope, github.ScaleSetID}
			if setIDs[id] {
				return nil, fmt.Errorf("conflicting scale set ID for profile %s", profile.ID)
			}
			setIDs[id] = true
		}
		for _, label := range github.Labels {
			key := selector{scope, strings.ToLower(label)}
			if selectors[key] {
				return nil, fmt.Errorf("conflicting GitHub selector for profile %s", profile.ID)
			}
			selectors[key] = true
		}
	}
	return scopes, nil
}

func (p Profile) validate() error {
	if p.Capacity.IdleReserve > p.Capacity.MaxRunners {
		return errors.New("idleReserve exceeds maxRunners")
	}
	if p.Provisioner.Type == "fargate" {
		if p.Purpose != "fargate-compatible" || p.Platform.OS != "linux" {
			return errors.New("runner execution on Fargate requires Linux and purpose fargate-compatible")
		}
	} else if p.Purpose == "fargate-compatible" ||
		(p.Purpose == "copilot-cloud-agent" && (p.Platform.OS != "linux" || p.Platform.Arch != "x64")) ||
		(p.Platform.OS == "windows" && p.Platform.Arch != "x64") {
		return errors.New("unsupported ASG purpose or platform")
	}
	labels := make(map[string]bool, len(p.GitHub.Labels))
	for _, label := range p.GitHub.Labels {
		key := strings.ToLower(label)
		if labels[key] {
			return errors.New("case-insensitive duplicate GitHub label")
		}
		labels[key] = true
	}
	if !labels[strings.ToLower(p.GitHub.ScaleSet)] {
		return errors.New("GitHub labels must include the scale set name")
	}
	return nil
}

type subnetMapping struct{ vpc, zone string }
type trustBoundary struct {
	scope, group, purpose, os, arch, container, bootstrapRole, executionRole string
}

type deploymentBindings struct {
	spec            Spec
	clusters        map[string]string
	clusterOwners   map[string]bool
	subnets         map[string]subnetMapping
	groupVPCs       map[string]string
	asgOwners       map[string]bool
	taskDefinitions map[string]trustBoundary
}

func (b *deploymentBindings) checkHost() (map[string]bool, error) {
	host := b.spec.Controller.Hosting
	roles := make(map[string]bool)
	if host.Type != "external" {
		if err := b.useNetwork(host.Network, "controller host"); err != nil {
			return nil, err
		}
		if host.Type == "ecs-fargate" {
			if err := b.useCluster(host.Cluster, "controller host"); err != nil {
				return nil, err
			}
			if !fargateMemoryValid(host.CPU, host.MemoryMiB) {
				return nil, errors.New("controller host has an invalid Fargate CPU and memory combination")
			}
		}
		return roles, nil
	}
	identity := host.Identity
	if identity.Type == "roles-anywhere" {
		for _, arn := range []string{identity.TrustAnchorARN, identity.ProfileARN, identity.RoleARN} {
			if err := b.spec.AWS.checkARN(arn, "controller Roles Anywhere identity"); err != nil {
				return nil, err
			}
		}
		roles[identity.RoleARN] = true
		return roles, nil
	}
	roles[identity.PrincipalRoleARN] = true
	effectiveRole := identity.PrincipalRoleARN
	if identity.AssumeRoleARN != "" {
		effectiveRole = identity.AssumeRoleARN
		roles[effectiveRole] = true
	}
	if err := b.spec.AWS.checkARN(effectiveRole, "external EC2 role"); err != nil {
		return nil, err
	}
	return roles, nil
}

func (b *deploymentBindings) checkExecution(profile Profile, scope string, controllerRoles map[string]bool) error {
	provider := profile.Provisioner
	if err := b.spec.AWS.checkARN(provider.BootstrapRoleARN, "runner bootstrap role"); err != nil {
		return err
	}
	if controllerRoles[provider.BootstrapRoleARN] {
		return fmt.Errorf("profile %s uses a controller role for runner bootstrap", profile.ID)
	}
	if provider.Type == "asg" {
		return b.useASG(profile)
	}
	if err := b.checkFargate(profile, controllerRoles); err != nil {
		return err
	}
	boundary := trustBoundary{
		scope:         scope,
		group:         strings.ToLower(profile.GitHub.RunnerGroup),
		purpose:       profile.Purpose,
		os:            profile.Platform.OS,
		arch:          profile.Platform.Arch,
		container:     provider.RunnerContainer,
		bootstrapRole: provider.BootstrapRoleARN,
		executionRole: provider.ExecutionRoleARN,
	}
	if previous, ok := b.taskDefinitions[provider.TaskDefinitionARN]; ok && previous != boundary {
		return fmt.Errorf("profile %s shares a task definition across incompatible runner profiles", profile.ID)
	}
	b.taskDefinitions[provider.TaskDefinitionARN] = boundary
	return nil
}

func (b *deploymentBindings) useASG(profile Profile) error {
	arn := profile.Provisioner.GroupARN
	if err := b.spec.AWS.checkARN(arn, "ASG ARN"); err != nil {
		return err
	}
	if b.asgOwners[arn] {
		return fmt.Errorf("conflicting ASG mapping for profile %s", profile.ID)
	}
	b.asgOwners[arn] = true
	return nil
}

func (b *deploymentBindings) checkFargate(profile Profile, controllerRoles map[string]bool) error {
	provider := profile.Provisioner
	if err := b.useNetwork(provider.Network, "profile "+profile.ID); err != nil {
		return err
	}
	if err := b.useCluster(provider.Cluster, "profile "+profile.ID); err != nil {
		return err
	}
	if err := b.spec.AWS.checkARN(provider.TaskDefinitionARN, "task definition ARN"); err != nil {
		return err
	}
	if err := b.spec.AWS.checkARN(provider.ExecutionRoleARN, "task execution role"); err != nil {
		return err
	}
	if controllerRoles[provider.ExecutionRoleARN] || provider.ExecutionRoleARN == provider.BootstrapRoleARN {
		return fmt.Errorf("profile %s requires a distinct task execution role", profile.ID)
	}
	return nil
}

var zoneSuffix = regexp.MustCompile(`^([a-z]|-[a-z0-9]+(-[a-z0-9]+)*)$`)

func (b *deploymentBindings) useNetwork(name, field string) error {
	network, ok := b.spec.Networks[name]
	if !ok {
		return fmt.Errorf("%s references missing network %s", field, name)
	}
	subnetIDs := make(map[string]bool)
	for _, subnet := range network.Subnets {
		if subnetIDs[subnet.ID] {
			return fmt.Errorf("conflicting subnet ID: %s", subnet.ID)
		}
		subnetIDs[subnet.ID] = true
		suffix, found := strings.CutPrefix(subnet.AZ, b.spec.AWS.Region)
		if !found || !zoneSuffix.MatchString(suffix) {
			return fmt.Errorf("%s has a subnet outside the configured Region", field)
		}
		mapping := subnetMapping{network.VPCID, subnet.AZ}
		if previous, ok := b.subnets[subnet.ID]; ok && previous != mapping {
			return fmt.Errorf("conflicting subnet mapping: %s", subnet.ID)
		}
		b.subnets[subnet.ID] = mapping
	}
	for _, group := range network.SecurityGroupIDs {
		if previous, ok := b.groupVPCs[group]; ok && previous != network.VPCID {
			return fmt.Errorf("conflicting security group mapping: %s", group)
		}
		b.groupVPCs[group] = network.VPCID
	}
	return nil
}

func (b *deploymentBindings) useCluster(name, field string) error {
	if _, ok := b.clusters[name]; ok {
		return nil
	}
	cluster, ok := b.spec.ECSClusters[name]
	if !ok {
		return fmt.Errorf("%s references missing ECS cluster %s", field, name)
	}
	arn := cluster.ARN
	if cluster.Type == "create" {
		aws := b.spec.AWS
		arn = fmt.Sprintf("arn:%s:ecs:%s:%s:cluster/%s", aws.partition(), aws.Region, aws.Account, cluster.Name)
	}
	if err := b.spec.AWS.checkARN(arn, field+" ECS cluster"); err != nil {
		return err
	}
	if b.clusterOwners[arn] {
		return fmt.Errorf("conflicting ECS cluster mapping for %s", name)
	}
	b.clusterOwners[arn] = true
	b.clusters[name] = arn
	return nil
}

var scopePath = regexp.MustCompile(`^/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)?/?$`)

// CanonicalScope validates the GitHub URL and normalizes its routing identity.
func (c GitHubConnection) CanonicalScope() (string, error) {
	u, err := url.Parse(c.ScopeURL)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Hostname() == "" ||
		u.User != nil || strings.ContainsAny(c.ScopeURL, "?#") ||
		(u.Port() != "" && u.Port() != "443") {
		return "", errors.New("expected a canonical HTTPS scope URL")
	}
	path := strings.ToLower(strings.TrimSuffix(u.EscapedPath(), "/"))
	if !scopePath.MatchString(path) {
		return "", errors.New("expected an organisation, repository or enterprise path")
	}
	enterprise := strings.HasPrefix(path, "/enterprises/")
	if enterprise != (c.Auth.Type == "enterprise-pat") {
		return "", errors.New("credential type is incompatible with the scope")
	}
	host := strings.ToLower(u.Hostname())
	if host == "www.github.com" {
		host = "github.com"
	}
	if host != "github.com" && !strings.HasSuffix(host, ".ghe.com") && c.CABundlePath == "" {
		return "", errors.New("custom GHES host requires caBundlePath")
	}
	return "https://" + host + path, nil
}

func (a AWS) partition() string {
	switch {
	case strings.HasPrefix(a.Region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(a.Region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(a.Region, "us-isob-"):
		return "aws-iso-b"
	case strings.HasPrefix(a.Region, "us-iso-"):
		return "aws-iso"
	case strings.HasPrefix(a.Region, "eu-isoe-"):
		return "aws-iso-e"
	case strings.HasPrefix(a.Region, "us-isof-"):
		return "aws-iso-f"
	case strings.HasPrefix(a.Region, "eusc-"):
		return "aws-eusc"
	default:
		return "aws"
	}
}

func (a AWS) checkARN(arn, field string) error {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[1] != a.partition() || parts[4] != a.Account ||
		(parts[3] != "" && parts[3] != a.Region) {
		return fmt.Errorf("%s must belong to the configured AWS account and Region", field)
	}
	return nil
}

func fargateMemoryValid(cpu, memory int) bool {
	if cpu == 256 {
		return memory == 512 || memory == 1024 || memory == 2048
	}
	bounds := map[int]struct{ min, max, step int }{
		512:   {1024, 4096, 1024},
		1024:  {2048, 8192, 1024},
		2048:  {4096, 16384, 1024},
		4096:  {8192, 30720, 1024},
		8192:  {16384, 61440, 4096},
		16384: {32768, 122880, 8192},
	}
	rangeForCPU, ok := bounds[cpu]
	return ok && memory >= rangeForCPU.min && memory <= rangeForCPU.max &&
		(memory-rangeForCPU.min)%rangeForCPU.step == 0
}
