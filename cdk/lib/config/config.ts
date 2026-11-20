import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { Ajv2020 } from "ajv/dist/2020.js";
import { isScalar, parseAllDocuments, visit } from "yaml";
import configSchema from "../../config/schema/controller-config-v1alpha1.json" with { type: "json" };
import runtimeSchema from "../../config/schema/controller-runtime-v1alpha1.json" with { type: "json" };
import type {
  EnvironmentConfig,
  GitHubConnection,
  RunnerProfile,
  RuntimeConfig,
} from "./types.ts";

const ajv = new Ajv2020({ allErrors: true, ownProperties: true });
const validateConfig = ajv.compile<EnvironmentConfig>(configSchema);
const validateRuntime = ajv.compile<RuntimeConfig>(runtimeSchema);

const fargateMemory = new Map([
  [256, [512, 1024, 2048]],
  [512, [1024, 2048, 3072, 4096]],
  [1024, memorySizes(2048, 8192, 1024)],
  [2048, memorySizes(4096, 16384, 1024)],
  [4096, memorySizes(8192, 30720, 1024)],
  [8192, memorySizes(16384, 61440, 4096)],
  [16384, memorySizes(32768, 122880, 8192)],
]);

function memorySizes(min: number, max: number, step: number): number[] {
  return Array.from(
    { length: (max - min) / step + 1 },
    (_, i) => min + i * step,
  );
}

function parseYaml(source: string): unknown {
  const docs = parseAllDocuments(source, {
    version: "1.2",
    strict: true,
    uniqueKeys: true,
    merge: false,
    prettyErrors: false,
  });
  if (docs.length !== 1) {
    throw new Error("Expected exactly one YAML document");
  }
  const doc = docs[0];
  const errors = [...doc.errors, ...doc.warnings];
  if (errors.length > 0) {
    const issue = errors[0];
    throw new Error(`Invalid YAML (${issue.code} at offset ${issue.pos[0]})`);
  }
  if (doc.directives?.yaml.version !== "1.2") {
    throw new Error("Expected YAML 1.2");
  }
  visit(doc, {
    Alias() {
      throw new Error("YAML aliases are not supported");
    },
    Pair(_key, pair) {
      if (!isScalar(pair.key) || typeof pair.key.value !== "string") {
        throw new Error("YAML mapping keys must be strings");
      }
      if (pair.key.value === "<<") {
        throw new Error("YAML merge keys are not supported");
      }
    },
    Node(_key, node) {
      if (node.tag && !node.tag.startsWith("tag:yaml.org,2002:")) {
        throw new Error("Custom YAML tags are not supported");
      }
    },
  });
  return doc.toJS({ maxAliasCount: 0 });
}

function awsPartition(region: string): string {
  if (region.startsWith("cn-")) return "aws-cn";
  if (region.startsWith("us-gov-")) return "aws-us-gov";
  if (region.startsWith("us-isob-")) return "aws-iso-b";
  if (region.startsWith("us-iso-")) return "aws-iso";
  if (region.startsWith("eu-isoe-")) return "aws-iso-e";
  if (region.startsWith("us-isof-")) return "aws-iso-f";
  if (region.startsWith("eusc-")) return "aws-eusc";
  return "aws";
}

function checkAwsArn(
  arn: string,
  aws: EnvironmentConfig["spec"]["aws"],
  field: string,
) {
  const [, partition, , region, account] = arn.split(":", 5);
  if (
    partition !== awsPartition(aws.region) ||
    account !== aws.account ||
    (region && region !== aws.region)
  ) {
    throw new Error(
      `${field} must belong to the configured AWS account and Region`,
    );
  }
}

function canonicalScope(connection: GitHubConnection, name: string): string {
  const scope = URL.parse(connection.scopeURL);
  if (
    !scope ||
    scope.protocol !== "https:" ||
    !scope.hostname ||
    scope.username ||
    scope.password ||
    scope.search ||
    scope.hash ||
    scope.port
  ) {
    throw new Error(
      `GitHub connection ${name} requires a canonical HTTPS scope URL`,
    );
  }
  if (!/^\/[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)?\/?$/.test(scope.pathname)) {
    throw new Error(
      `GitHub connection ${name} requires an organisation, repository or enterprise path`,
    );
  }
  const path = scope.pathname.replace(/\/$/, "").toLowerCase();
  const enterprise = path.startsWith("/enterprises/");
  if (enterprise !== (connection.auth.type === "enterprise-pat")) {
    throw new Error(
      `GitHub connection ${name} has an incompatible credential type for its scope`,
    );
  }
  const host =
    scope.hostname === "www.github.com" ? "github.com" : scope.hostname;
  const hosted = host === "github.com" || host.endsWith(".ghe.com");
  if (!hosted && !connection.caBundlePath) {
    throw new Error(
      `GitHub connection ${name} requires caBundlePath for its GHES host`,
    );
  }
  return `https://${host}${path}`;
}

function configured<T>(
  catalog: Record<string, T> | undefined,
  name: string,
  field: string,
): T {
  if (!catalog || !Object.hasOwn(catalog, name)) {
    throw new Error(`${field} references missing entry ${name}`);
  }
  return catalog[name];
}

function checkPlatform(profile: RunnerProfile) {
  const { os, arch } = profile.platform;
  if (profile.provisioner.type === "fargate") {
    if (profile.purpose !== "fargate-compatible" || os !== "linux") {
      throw new Error(
        `Profile ${profile.id} requires Linux and purpose fargate-compatible for Fargate`,
      );
    }
  } else if (
    profile.purpose === "fargate-compatible" ||
    (profile.purpose === "copilot-cloud-agent" &&
      (os !== "linux" || arch !== "x64")) ||
    (os === "windows" && arch !== "x64")
  ) {
    throw new Error(
      `Profile ${profile.id} has an unsupported ASG purpose or platform`,
    );
  }
}

function unique(seen: Set<string>, value: string, field: string) {
  if (seen.has(value)) {
    throw new Error(`Conflicting ${field}: ${value}`);
  }
  seen.add(value);
}

function sorted(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sorted);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sorted((value as Record<string, unknown>)[key])]),
    );
  }
  return value;
}

function checkGitHubProfiles(spec: EnvironmentConfig["spec"]) {
  const scopeByConnection = new Map(
    Object.entries(spec.github).map(([name, connection]) => [
      name,
      canonicalScope(connection, name),
    ]),
  );
  const profileIDs = new Set<string>();
  const scaleSetOwners = new Set<string>();
  const selectorOwners = new Set<string>();
  for (const profile of spec.profiles) {
    unique(profileIDs, profile.id, "profile ID");
    const { github, capacity } = profile;
    if (capacity.idleReserve > capacity.maxRunners) {
      throw new Error(`Profile ${profile.id} idleReserve exceeds maxRunners`);
    }
    checkPlatform(profile);
    const labels = github.labels.map((label) => label.toLowerCase());
    if (
      new Set(labels).size !== labels.length ||
      !labels.includes(github.scaleSet.toLowerCase())
    ) {
      throw new Error(
        `Profile ${profile.id} needs unique labels including its scale set name`,
      );
    }
    if (profile.state === "disabled") {
      continue;
    }

    const connection = configured(
      spec.github,
      github.connection,
      `Profile ${profile.id}`,
    );
    checkAwsArn(connection.auth.secretArn, spec.aws, "GitHub secret ARN");
    const scope = scopeByConnection.get(github.connection);
    unique(
      scaleSetOwners,
      JSON.stringify([
        scope,
        github.runnerGroup.toLowerCase(),
        github.scaleSet.toLowerCase(),
      ]),
      "GitHub scale set mapping",
    );
    if (github.scaleSetId !== undefined) {
      unique(
        scaleSetOwners,
        JSON.stringify([scope, github.scaleSetId]),
        "GitHub scale set ID",
      );
    }
    for (const label of labels) {
      unique(selectorOwners, `${scope}/${label}`, "GitHub selector");
    }
  }
  return scopeByConnection;
}

function selectedClusters(
  spec: EnvironmentConfig["spec"],
  scopeByConnection: Map<string, string>,
) {
  const { aws } = spec;
  const { region } = aws;
  const enabledClusters = new Map<string, string>();
  const clusterOwners = new Set<string>();
  const subnetMappings = new Map<string, string>();
  const securityGroupVpcs = new Map<string, string>();
  const useCluster = (name: string, field: string) => {
    if (enabledClusters.has(name)) {
      return;
    }
    const cluster = configured(spec.ecsClusters, name, field);
    const arn =
      cluster.type === "import"
        ? cluster.arn
        : `arn:${awsPartition(region)}:ecs:${region}:${aws.account}:cluster/${cluster.name}`;
    checkAwsArn(arn, aws, field);
    unique(clusterOwners, arn, "ECS cluster mapping");
    enabledClusters.set(name, arn);
  };
  const useNetwork = (name: string, field: string) => {
    const network = configured(spec.networks, name, field);
    const subnets = new Set<string>();
    for (const subnet of network.subnets) {
      unique(subnets, subnet.id, "subnet ID");
      const zoneSuffix = subnet.az.slice(region.length);
      if (
        !subnet.az.startsWith(region) ||
        !/^(?:[a-z]|-[a-z0-9]+(?:-[a-z0-9]+)*)$/.test(zoneSuffix)
      ) {
        throw new Error(`${field} has a subnet outside the configured Region`);
      }
      const mapping = network.vpcId + "/" + subnet.az;
      if (
        subnetMappings.has(subnet.id) &&
        subnetMappings.get(subnet.id) !== mapping
      ) {
        throw new Error(`Conflicting subnet mapping: ${subnet.id}`);
      }
      subnetMappings.set(subnet.id, mapping);
    }
    for (const group of network.securityGroupIds) {
      if (
        securityGroupVpcs.has(group) &&
        securityGroupVpcs.get(group) !== network.vpcId
      ) {
        throw new Error(`Conflicting security group mapping: ${group}`);
      }
      securityGroupVpcs.set(group, network.vpcId);
    }
  };

  const host = spec.controller.hosting;
  const controllerRoles = new Set<string>();
  if (host.type !== "external") {
    useNetwork(host.network, "Controller host");
    if (host.type === "ecs-fargate") {
      useCluster(host.cluster, "Controller host");
      if (!fargateMemory.get(host.cpu)?.includes(host.memoryMiB)) {
        throw new Error(
          "Controller host has an invalid Fargate CPU and memory combination",
        );
      }
    }
  } else if (host.identity.type === "roles-anywhere") {
    for (const arn of [
      host.identity.trustAnchorArn,
      host.identity.profileArn,
      host.identity.roleArn,
    ]) {
      checkAwsArn(arn, aws, "Controller Roles Anywhere identity");
    }
    controllerRoles.add(host.identity.roleArn);
  } else {
    checkAwsArn(
      host.identity.assumeRoleArn ?? host.identity.principalRoleArn,
      aws,
      "External EC2 role",
    );
    controllerRoles.add(host.identity.principalRoleArn);
    if (host.identity.assumeRoleArn) {
      controllerRoles.add(host.identity.assumeRoleArn);
    }
  }

  const asgOwners = new Set<string>();
  const taskDefinitions = new Map<string, string>();
  function checkFargate(
    profile: RunnerProfile,
    provisioner: Extract<RunnerProfile["provisioner"], { type: "fargate" }>,
  ) {
    const { github } = profile;
    const scope = scopeByConnection.get(github.connection);
    useNetwork(provisioner.network, `Profile ${profile.id}`);
    useCluster(provisioner.cluster, `Profile ${profile.id}`);
    checkAwsArn(provisioner.taskDefinitionArn, aws, "Task definition ARN");
    checkAwsArn(provisioner.executionRoleArn, aws, "Task execution role");
    if (
      controllerRoles.has(provisioner.executionRoleArn) ||
      provisioner.executionRoleArn === provisioner.bootstrapRoleArn
    ) {
      throw new Error(
        `Profile ${profile.id} requires a distinct task execution role`,
      );
    }
    const boundary = JSON.stringify([
      scope,
      github.runnerGroup.toLowerCase(),
      profile.purpose,
      profile.platform.os,
      profile.platform.arch,
      provisioner.runnerContainer,
      provisioner.bootstrapRoleArn,
      provisioner.executionRoleArn,
    ]);
    const previous = taskDefinitions.get(provisioner.taskDefinitionArn);
    if (previous !== undefined && previous !== boundary) {
      throw new Error(
        `Profile ${profile.id} shares its task definition across incompatible runner profiles`,
      );
    }
    taskDefinitions.set(provisioner.taskDefinitionArn, boundary);
  }

  for (const profile of spec.profiles) {
    if (profile.state === "disabled") {
      continue;
    }
    const { provisioner } = profile;
    checkAwsArn(provisioner.bootstrapRoleArn, aws, "Runner bootstrap role");
    if (controllerRoles.has(provisioner.bootstrapRoleArn)) {
      throw new Error(
        `Profile ${profile.id} uses a controller role for runner bootstrap`,
      );
    }
    if (provisioner.type === "asg") {
      checkAwsArn(provisioner.groupArn, aws, "ASG ARN");
      unique(asgOwners, provisioner.groupArn, "ASG mapping");
      continue;
    }
    checkFargate(profile, provisioner);
  }
  return enabledClusters;
}

export function loadEnvironmentConfig(path: string) {
  return compileEnvironmentConfig(readFileSync(path, "utf8"));
}

export function compileEnvironmentConfig(source: string) {
  const value = parseYaml(source);
  if (!validateConfig(value)) {
    const errors = validateConfig.errors?.map(
      (error) => `${error.instancePath || "/"} ${error.message}`,
    );
    throw new Error(`Invalid configuration: ${errors?.join(", ")}`);
  }

  const spec = value.spec;
  const scopes = checkGitHubProfiles(spec);
  const enabledClusters = selectedClusters(spec, scopes);
  const config: EnvironmentConfig = {
    ...value,
    spec: {
      ...spec,
      mode: spec.mode ?? "dry-run",
      dryRun: {
        inspectAws: spec.dryRun?.inspectAws ?? false,
        demand: spec.dryRun?.demand ?? { type: "none" },
      },
      profiles: spec.profiles.map((profile) => ({
        ...profile,
        github: {
          ...profile.github,
          registration: profile.github.registration ?? "existing",
        },
      })),
    },
  };
  const table = spec.tracking.table;
  const runtime: RuntimeConfig = {
    apiVersion: value.apiVersion,
    kind: "RunnerControllerRuntime",
    config,
    resources: {
      trackingTableName:
        table.type === "import"
          ? table.name
          : `${value.metadata.name}-runner-lifecycle`,
      ecsClusterArns: Object.fromEntries(enabledClusters),
    },
  };
  if (!validateRuntime(runtime)) {
    throw new Error("Resolved runtime configuration does not match its schema");
  }
  const json = JSON.stringify(sorted(runtime)) + "\n";
  return {
    runtime,
    json,
    sha256: createHash("sha256").update(json).digest("hex"),
  };
}
