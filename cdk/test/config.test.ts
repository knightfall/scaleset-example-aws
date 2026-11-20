import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { stringify } from "yaml";
import {
  compileEnvironmentConfig,
  loadEnvironmentConfig,
} from "../lib/config/config.ts";

const fixtures = fileURLToPath(
  new URL("../../testdata/config/", import.meta.url),
);
const asg = join(fixtures, "asg-only-external.yaml");
const fargate = join(fixtures, "fargate-only-import.yaml");
const mixed = join(fixtures, "mixed-ecs-host.yaml");
const base = readFileSync(asg, "utf8");

test("the authored fixtures produce the shared runtime fixtures", () => {
  for (const name of [
    "asg-only-external",
    "fargate-only-import",
    "mixed-ecs-host",
  ]) {
    const expected = JSON.parse(
      readFileSync(join(fixtures, "runtime", `${name}.json`), "utf8"),
    );
    const { runtime } = loadEnvironmentConfig(join(fixtures, `${name}.yaml`));
    assert.deepEqual(runtime, expected, name);
  }
});

const rejections: {
  name: string;
  fixture: string;
  path: string;
  value: string | number;
  yamlError: string;
}[] = JSON.parse(readFileSync(join(fixtures, "rejections.json"), "utf8"));

for (const rejection of rejections) {
  test(`shared rejection: ${rejection.name}`, () => {
    const { runtime } = loadEnvironmentConfig(
      join(fixtures, `${rejection.fixture}.yaml`),
    );
    const path = rejection.path.split(".");
    const key = path.pop();
    assert.ok(key);
    const configuration: Record<string, unknown> = { ...runtime.config };
    let object = configuration;
    for (const item of path) object = object[item] as Record<string, unknown>;
    object[key] = rejection.value;
    assert.throws(
      () => compileEnvironmentConfig(stringify(configuration)),
      (error: Error) => {
        assert.ok(error.message.includes(rejection.yamlError), error.message);
        return true;
      },
    );
  });
}

test("ASG-only YAML resolves without ECS settings and defaults to dry-run", () => {
  const { runtime, json, sha256 } = loadEnvironmentConfig(asg);
  assert.equal(runtime.config.spec.mode, "dry-run");
  assert.deepEqual(runtime.config.spec.dryRun, {
    inspectAws: false,
    demand: { type: "none" },
  });
  assert.equal(runtime.config.spec.profiles[0].github.registration, "existing");
  assert.equal(runtime.config.spec.profiles[0].provisioner.type, "asg");
  assert.equal(
    runtime.resources.trackingTableName,
    "engineering-dev-runner-lifecycle",
  );
  assert.deepEqual(runtime.resources.ecsClusterArns, {});
  assert.equal(sha256, createHash("sha256").update(json).digest("hex"));
  assert.deepEqual(JSON.parse(json), runtime);
});

test("Fargate-only YAML resolves an existing cluster independently of its host", () => {
  const { config, resources } = loadEnvironmentConfig(fargate).runtime;
  assert.equal(config.spec.profiles[0].provisioner.type, "fargate");
  assert.equal(config.spec.github.corporate.auth.type, "enterprise-pat");
  assert.equal(config.spec.controller.hosting.type, "ecs-fargate");
  assert.equal(resources.trackingTableName, "existing-runner-lifecycle");
  assert.deepEqual(resources.ecsClusterArns, {
    engineering: "arn:aws:ecs:ap-southeast-2:111122223333:cluster/engineering",
  });
  config.spec.controller.hosting =
    loadEnvironmentConfig(asg).runtime.config.spec.controller.hosting;
  const external = compileEnvironmentConfig(stringify(config)).runtime;
  assert.equal(external.config.spec.controller.hosting.type, "external");
  assert.deepEqual(external.resources.ecsClusterArns, resources.ecsClusterArns);
});

test("mixed YAML resolves an imported host cluster and a distinct new runner cluster", () => {
  const { runtime } = loadEnvironmentConfig(mixed);
  assert.deepEqual(
    runtime.config.spec.profiles.map((profile) => profile.provisioner.type),
    ["asg", "fargate"],
  );
  assert.deepEqual(runtime.resources.ecsClusterArns, {
    engineering: "arn:aws:ecs:ap-southeast-2:111122223333:cluster/engineering",
    "runner-new":
      "arn:aws:ecs:ap-southeast-2:111122223333:cluster/engineering-dev-runners",
  });
});

test("disabled Fargate profiles do not require a network or cluster", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  const disabled =
    loadEnvironmentConfig(fargate).runtime.config.spec.profiles[0];
  disabled.state = "disabled";
  config.spec.profiles.push(disabled);
  const runtime = compileEnvironmentConfig(stringify(config)).runtime;
  assert.deepEqual(runtime.resources.ecsClusterArns, {});
  assert.equal(runtime.config.spec.profiles[1].state, "disabled");
});

test("managed EC2 hosting uses its own network without selecting ECS", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  config.spec.networks =
    loadEnvironmentConfig(fargate).runtime.config.spec.networks;
  config.spec.controller.hosting = {
    type: "ec2",
    network: "controller-private",
    amiId: "ami-0123456789abcdef0",
    instanceType: "m7i.large",
  };
  const runtime = compileEnvironmentConfig(stringify(config)).runtime;
  assert.equal(runtime.config.spec.controller.hosting.type, "ec2");
  assert.deepEqual(runtime.resources.ecsClusterArns, {});
});

test("comments and mapping key order produce identical runtime bytes and digest", () => {
  const first = compileEnvironmentConfig(base);
  const { apiVersion, kind, metadata, spec } = first.runtime.config;
  const { aws, controller, tracking, github, profiles } = spec;
  const reordered = stringify({
    spec: { profiles, github, tracking, controller, aws },
    metadata,
    kind,
    apiVersion,
  });
  const second = compileEnvironmentConfig("# Equivalent config\n" + reordered);
  assert.equal(second.json, first.json);
  assert.equal(second.sha256, first.sha256);
});

for (const [name, source, expected] of [
  ["duplicate key", base + "kind: RunnerController\n", /DUPLICATE_KEY/],
  ["unknown key", base + "unknown: value\n", /additional properties/],
  [
    "extra document",
    base + "---\nother: document\n",
    /exactly one YAML document/,
  ],
  ["alias", base + "bad: *alias\n", /aliases/],
  ["merge key", base + "<<: { a: b }\n", /merge keys/],
  ["numeric key", base + "42: value\n", /mapping keys must be strings/],
  [
    "custom tag",
    base + "bad: !secret token\n",
    /TAG_RESOLVE_FAILED|Custom YAML tags/,
  ],
  ["unknown API version", base.replace("v1alpha1", "v2"), /apiVersion/],
  [
    "inline token",
    base.replace(
      "        secretArn:",
      "        token: private-fixture-token\n        secretArn:",
    ),
    /additional properties/,
  ],
] as const) {
  test(`rejects ${name}`, () => {
    assert.throws(
      () => compileEnvironmentConfig(source),
      (error: Error) => {
        assert.match(error.message, expected);
        assert.doesNotMatch(error.message, /private-fixture-token/);
        return true;
      },
    );
  });
}

test("rejects an empty or all-disabled profile set", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  for (const profile of config.spec.profiles) profile.state = "disabled";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /must contain at least 1 valid item/,
  );
  config.spec.profiles = [];
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /must NOT have fewer than 1 items/,
  );
});

test("rejects an AWS backend union with fields from another backend", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  Object.assign(config.spec.profiles[0].provisioner, {
    cluster: "engineering",
  });
  const source = stringify(config);
  assert.throws(() => compileEnvironmentConfig(source), /provisioner/);
});

test("rejects conflicting ASGs and case-insensitive selector ownership", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  const other = structuredClone(config.spec.profiles[0]);
  other.id = "other";
  other.github.scaleSet = "Linux-Docker";
  other.github.runnerGroup = "Another Group";
  config.spec.profiles.push(other);
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /Conflicting GitHub selector/,
  );
  other.github.scaleSet = "other-docker";
  other.github.labels = ["other-docker"];
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /Conflicting ASG mapping/,
  );
});

test("rejects missing selected resources and mismatched account or Region", () => {
  const { config } = loadEnvironmentConfig(fargate).runtime;
  const clusters = config.spec.ecsClusters;
  delete config.spec.ecsClusters;
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /missing entry engineering/,
  );
  config.spec.ecsClusters = clusters;
  const cluster = config.spec.ecsClusters?.engineering;
  assert.ok(cluster?.type === "import");
  cluster.arn = cluster.arn.replace("ap-southeast-2", "us-east-1");
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /configured AWS account and Region/,
  );
});

test("rejects invalid capacity and platform combinations", () => {
  const { config } = loadEnvironmentConfig(fargate).runtime;
  const profile = config.spec.profiles[0];
  profile.capacity.idleReserve = profile.capacity.maxRunners + 1;
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /idleReserve exceeds maxRunners/,
  );
  profile.capacity.idleReserve = 0;
  profile.platform.os = "windows";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /requires Linux/,
  );
  profile.platform.os = "linux";
  const host = config.spec.controller.hosting;
  assert.equal(host.type, "ecs-fargate");
  if (host.type !== "ecs-fargate") throw new Error("Unexpected host fixture");
  host.memoryMiB = 1536;
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /invalid Fargate CPU and memory/,
  );
});

test("rejects an active runner that shares the external controller's role", () => {
  const { config } = loadEnvironmentConfig(asg).runtime;
  const host = config.spec.controller.hosting;
  assert.ok(
    host.type === "external" && host.identity.type === "ec2-instance-profile",
  );
  config.spec.profiles[0].provisioner.bootstrapRoleArn =
    host.identity.principalRoleArn;
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /controller role for runner bootstrap/,
  );
});

test("rejects conflicting subnet and security group mappings", () => {
  const { config } = loadEnvironmentConfig(fargate).runtime;
  const networks = config.spec.networks;
  assert.ok(networks);
  const host = networks["controller-private"];
  const runner = networks["runner-private"];
  const originalSubnet = runner.subnets[0].id;
  runner.subnets[0].id = host.subnets[0].id;
  runner.vpcId = "vpc-0123456789abcdef1";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /Conflicting subnet mapping/,
  );
  runner.subnets[0].id = originalSubnet;
  runner.securityGroupIds = [...host.securityGroupIds];
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /Conflicting security group mapping/,
  );
});

test("validates GitHub authentication scopes and GHES CA paths", () => {
  const { config } = loadEnvironmentConfig(fargate).runtime;
  const corporate = config.spec.github.corporate;
  corporate.scopeURL =
    "https://github.example.invalid/enterprises/example-corporate";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /requires caBundlePath/,
  );
  corporate.caBundlePath = "/etc/ssl/certs/enterprise.pem";
  assert.doesNotThrow(() => compileEnvironmentConfig(stringify(config)));
  corporate.scopeURL = "https://github.com/example-org";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /incompatible credential type/,
  );
  corporate.scopeURL =
    "https://fixture-user:fixture-pass@github.com/enterprises/example-corporate";
  assert.throws(
    () => compileEnvironmentConfig(stringify(config)),
    /canonical HTTPS scope URL/,
  );
});

test("CLI emits the validated JSON and prints its digest", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "scaleset-config-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const artifact = join(directory, "runtime.json");
  const cli = fileURLToPath(new URL("../bin/emit-runtime.ts", import.meta.url));
  const args = [cli, asg, artifact];
  const output = execFileSync(process.execPath, args, { encoding: "utf8" });
  const { json, sha256 } = loadEnvironmentConfig(asg);
  assert.equal(readFileSync(artifact, "utf8"), json);
  assert.equal(output.trim(), sha256);
  const existing = spawnSync(process.execPath, args);
  assert.equal(existing.status, 1);
  assert.equal(readFileSync(artifact, "utf8"), json);
});
