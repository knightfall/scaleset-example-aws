import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

test("the pinned CDK CLI synthesizes a TypeScript stack offline", (t) => {
  const cdk = fileURLToPath(new URL("..", import.meta.url));
  const out = mkdtempSync(join(tmpdir(), "scaleset-cdk-synth-"));
  t.after(() => rmSync(out, { recursive: true, force: true }));

  execFileSync(
    process.execPath,
    [
      "node_modules/aws-cdk/bin/cdk",
      "synth",
      "Toolchain",
      "--app",
      "node test/fixtures/toolchain-stack.ts",
      "--strict",
      "--no-lookups",
      "--notices=false",
      "--quiet",
      "--output",
      out,
    ],
    { cwd: cdk, env: { ...process.env, AWS_EC2_METADATA_DISABLED: "true" } },
  );

  const template = JSON.parse(
    readFileSync(join(out, "Toolchain.template.json"), "utf8"),
  );
  assert.equal(template.Outputs.ToolchainReady.Value, "strict-offline-synth");
});
