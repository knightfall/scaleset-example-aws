# Agent Instructions

## Principles

- Read existing code before modifying it.
- Prefer simple, idiomatic solutions and minimal diffs.
- No speculative abstractions, unnecessary dependencies, or unrelated refactoring.
- No placeholder implementations, redundant comments, or generated boilerplate.
- Verify external SDK APIs; never invent functionality.

## Architecture

- Use the official `actions/scaleset` SDK and AWS SDK for Go v2.
- Keep runner profiles configuration-driven through validated YAML.
- Initialise only enabled AWS provisioners.
- Reuse existing infrastructure; avoid destructive CDK changes.
- Ensure provisioning and cleanup are idempotent and concurrency-safe.
- Never terminate active runners or expose credentials.

## Repository Layout

- Wire the single Go controller in the root `main.go`. Keep supporting packages flat and name them for the feature they own.
- Put Go SDK contract and other black-box suites in `tests/`. Keep a Go unit test next to its package when it needs access to unexported behavior.
- Keep CDK entry points in `cdk/bin/`, implementation in `cdk/lib/`, environment YAML and schemas in `cdk/config/`, and TypeScript tests in `cdk/test/`.
- Keep shared YAML and JSON test fixtures in `testdata/config/`.

## Code Quality

- **Go:** `gofmt`, `go vet`, `golangci-lint`, `go test -race ./...`.
- **TypeScript/CDK:** strict TypeScript, ESLint, Prettier, `tsc --noEmit`, `cdk synth`.
- Handle errors explicitly; never silently suppress lint failures.
- Write meaningful tests for changed behaviour.
- Pin tool versions and use the same checks in CI.

## Delivery

- Make the smallest complete, testable change.
- Run relevant checks before declaring completion.
- Report changes, validation results, and unresolved limitations.
- Never claim checks passed unless executed.
