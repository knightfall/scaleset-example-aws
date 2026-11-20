export type Architecture = "x64" | "arm64";

export type ExternalIdentity =
  | {
      type: "ec2-instance-profile";
      principalRoleArn: string;
      assumeRoleArn?: string;
    }
  | {
      type: "roles-anywhere";
      trustAnchorArn: string;
      profileArn: string;
      roleArn: string;
    };

export type Hosting =
  | { type: "external"; identity: ExternalIdentity }
  | { type: "ec2"; network: string; amiId: string; instanceType: string }
  | {
      type: "ecs-fargate";
      network: string;
      cluster: string;
      cpuArchitecture: Architecture;
      cpu: number;
      memoryMiB: number;
    };

export type DemandInput =
  | { type: "none" }
  | { type: "snapshot" | "replay"; path: string }
  | {
      type: "snapshot" | "replay";
      s3: { bucket: string; key: string; sha256: string; versionId?: string };
    };

export interface GitHubConnection {
  scopeURL: string;
  caBundlePath?: string;
  auth: {
    type: "github-app" | "enterprise-pat";
    secretArn: string;
  };
}

export type Provisioner =
  | {
      type: "asg";
      groupArn: string;
      capacityUnits:
        { type: "instances" } | { type: "fixed-weight"; weight: number };
      bootstrapRoleArn: string;
      bootstrapProtocol: "v1";
    }
  | {
      type: "fargate";
      cluster: string;
      network: string;
      taskDefinitionArn: string;
      runnerContainer: string;
      bootstrapRoleArn: string;
      executionRoleArn: string;
      bootstrapProtocol: "v1";
    };

export interface RunnerProfile {
  id: string;
  state: "active" | "draining" | "disabled";
  purpose: "docker" | "copilot-cloud-agent" | "fargate-compatible";
  platform: { os: "linux" | "windows"; arch: Architecture };
  capacity: {
    idleReserve: number;
    maxRunners: number;
    startupTimeoutSeconds: number;
  };
  github: {
    connection: string;
    runnerGroup: string;
    scaleSet: string;
    scaleSetId?: number;
    labels: string[];
    registration?: "existing" | "create-if-missing";
  };
  provisioner: Provisioner;
}

export interface EnvironmentConfig {
  apiVersion: "runners.example/v1alpha1";
  kind: "RunnerController";
  metadata: { name: string };
  spec: {
    aws: { account: string; region: string };
    mode?: "dry-run" | "active";
    dryRun?: { inspectAws?: boolean; demand?: DemandInput };
    controller: { image: string; hosting: Hosting };
    tracking: { table: { type: "create" } | { type: "import"; name: string } };
    github: Record<string, GitHubConnection>;
    profiles: RunnerProfile[];
    networks?: Record<
      string,
      {
        vpcId: string;
        subnets: { id: string; az: string }[];
        securityGroupIds: string[];
      }
    >;
    ecsClusters?: Record<
      string,
      { type: "create"; name: string } | { type: "import"; arn: string }
    >;
  };
}

export interface RuntimeConfig {
  apiVersion: EnvironmentConfig["apiVersion"];
  kind: "RunnerControllerRuntime";
  config: EnvironmentConfig;
  resources: {
    trackingTableName: string;
    ecsClusterArns: Record<string, string>;
  };
}
