# Agent Instructions

## Principles

- Read existing code before modifying it.
- Prefer simple, idiomatic solutions and minimal diffs.
- No speculative abstractions, unnecessary dependencies, or unrelated refactoring.
- No placeholder implementations, redundant comments, or generated boilerplate.
- Verify external SDK APIs; never invent functionality.

## Architecture

- Use the official `actions/scaleset` SDK and AWS SDK for Go v2.
- Keep reconciliation rules independent of provider SDK types. Keep SDK calls and request translation in provider and persistence adapters.
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
- Use guard clauses to keep the main path shallow. Use `range` for Go collection loops and braces for TypeScript loop bodies.
- Write meaningful tests for changed behaviour.
- Pin tool versions and use the same checks in CI.

## Clarity and Simplicity

- Do not optimise for syntactic cleverness. Prefer familiar, easily reviewable constructs when two implementations are otherwise equivalent. Replace readable code with shorter alternatives only when they materially improve clarity.
- Use modern, idiomatic Go when it improves clarity, not merely because a construct is shorter or newer.
- Prefer values by default. Use pointers intentionally for optionality, mutation, identity, or SDK contracts; preserve zero versus absent versus unknown.
- Prefer straightforward loops and early returns. Split dense conditions when they express distinct invariants or need actionable errors.
- Extract functions for clearer responsibilities, not arbitrary line limits. Add interfaces, helpers, and types only for a concrete need.
- Keep lifecycle transitions, ownership, concurrency, retries, and recovery explicit and auditable.
- Explain non-obvious constraints, not what the code already says. Avoid cosmetic refactors and unnecessary code churn.
- Before finishing, check whether another engineer can follow both the normal path and failure cases without reconstructing hidden control flow.

## Delivery

- Make the smallest complete, testable change.
- Run relevant checks before declaring completion.
- Report changes, validation results, and unresolved limitations.
- Never claim checks passed unless executed.
