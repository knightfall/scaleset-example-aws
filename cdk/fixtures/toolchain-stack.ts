import { App, CfnOutput, Stack } from "aws-cdk-lib";

const app = new App();
const stack = new Stack(app, "Toolchain");
new CfnOutput(stack, "ToolchainReady", { value: "strict-offline-synth" });
