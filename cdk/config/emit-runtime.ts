import { writeFileSync } from "node:fs";
import process from "node:process";
import { loadEnvironmentConfig } from "./config.ts";

const [source, output, ...extra] = process.argv.slice(2);
if (!source || !output || extra.length > 0) {
  process.stderr.write(
    "Usage: npm run config:emit -- <environment.yaml> <runtime.json>\n",
  );
  process.exitCode = 2;
} else {
  try {
    const { json, sha256 } = loadEnvironmentConfig(source);
    writeFileSync(output, json, { flag: "wx" });
    process.stdout.write(sha256 + "\n");
  } catch (error) {
    process.stderr.write(
      (error instanceof Error ? error.message : String(error)) + "\n",
    );
    process.exitCode = 1;
  }
}
