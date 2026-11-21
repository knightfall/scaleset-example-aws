# Controller task specifications

These files expand the remaining tasks in the [implementation plan](../implementation-plan.md) for engineering review. The repository baseline is `origin/main` at `6c73e5e16c29efcfcc39bc3fcf1c978098c93d2b`, including PR #54. Status below describes checked-in code and fixtures at that baseline. Test commands describe future acceptance evidence. They do not record a test run or live acceptance.

The root CLI verifies configuration and produces offline dry-run reports. The lifecycle store, credentials, listeners and provider-neutral inventory coordinator have fixture coverage. Active runtime wiring, real provisioners, secure JIT delivery, deployment stacks and live canaries remain to be delivered. Existing synthesis covers an offline toolchain fixture. [Root entry point](../../main.go) · [CDK test](../../cdk/test/toolchain.test.ts)

| Task | Specification | Baseline state |
| --- | --- | --- |
| T10 | [Reconciliation, safe dispatch and retirement](T10.md) | In progress. Inventory coordinator and unfinished-effect count merged. |
| T11 | [Secure one-job JIT handoff](T11.md) | Pending. SDK wire fixtures exist. |
| T12 | [Existing ASG preflight and inventory](T12.md) | Pending. SDK wire fixtures exist. |
| T13 | [ASG scaling and targeted retirement](T13.md) | Pending. |
| T14 | [EC2 runner bootstrap](T14.md) | Pending. |
| T15 | [Fargate preflight and inventory](T15.md) | Pending. SDK wire fixtures exist. |
| T16 | [Fargate launch and cleanup](T16.md) | Pending. |
| T17 | [Fargate runner bootstrap](T17.md) | Pending. |
| T18 | [Shared CDK infrastructure and IAM](T18.md) | Pending. CDK config compiler exists. |
| T19 | [Docker image, runtime and external hosting](T19.md) | Pending. Image bases are pinned. |
| T20 | [Dedicated EC2 controller hosting](T20.md) | Pending. |
| T21 | [ECS Fargate controller hosting](T21.md) | Pending. |
| T22 | [CDK and IAM synthesis matrix](T22.md) | Pending. Config matrix and toolchain synth exist. |
| T23 | [GitHub and enterprise PAT sandbox](T23.md) | Live proof pending. |
| T24 | [ASG sandbox canary](T24.md) | Live proof pending. |
| T25 | [Fargate sandbox canary](T25.md) | Live proof pending. |
| T26 | [Mixed operation, dry-run and rollback](T26.md) | Live proof pending. |
| T27 | [Migration and production activation readiness](T27.md) | Readiness review pending. Activation requires separate approval. |

## Implementation order

Start with the provider-neutral dispatch and recovery slice of [T10](T10.md). That slice can be exercised using fakes while active mode remains unavailable. [T11](T11.md) and the ASG and Fargate branches can build on that boundary. T12 and T15 provide read-only inventory for the final T10 wiring and T05’s optional AWS inspection. This separates the core prerequisite from its integration and avoids a dependency cycle. T19 connects the production entry point and packages the same image used by all three hosts. T18–T22 build and verify infrastructure, and T23–T27 require evidence from authorised environments.

Earlier task definitions remain in the [implementation plan](../implementation-plan.md#implementation-tasks). Their remaining obligations are assigned below so they stay in scope without duplicating completed work.

| Prior obligation | Remaining specification |
| --- | --- |
| Optional AWS and immutable S3 reads for dry-run | [T12](T12.md), [T15](T15.md), [T18](T18.md), [T19](T19.md), [T26](T26.md) |
| Retention, bounded unfinished-effect recovery and actual table proof | [T10](T10.md), [T18](T18.md), [T26](T26.md) |
| Active lifecycle runtime and old-owner fencing | [T10](T10.md), [T19](T19.md)–[T21](T21.md), [T26](T26.md) |
| Enterprise PAT, App, message recovery and admission ordering | [T23](T23.md), with environment approval in [T27](T27.md) |

## Review and delivery

- Use [AGENTS.md](../../AGENTS.md) and the [implementation plan](../implementation-plan.md) as the repository and architecture contracts. Recheck pinned SDK operations, types and retry settings when implementing a task. Update the plan if a review accepts a requirement change.
- Keep each implementation increment independently testable on its own branch and PR against `main`. Use focused checks during development and `make check` before delivery. Record the checks actually run. Merge after passing required checks and clearing review on the current PR commit.
- Evaluate capability switches independently. Enabled and draining profiles determine runner adapters and permissions. Controller hosting controls its own resources. Disabled profiles, unused clusters and unselected backend services must not become startup dependencies. Dry-run must have a separate read-only dependency graph.
- Treat absent, stale, conflicting and unavailable evidence as separate from verified zero or terminal state. Keep job demand sourced from GitHub. Protect active and unknown work. An expired lease alone does not fence external AWS or GitHub calls.
- Review implementation acceptance separately from live service proof. Where a service guarantee is unverified, retain the conservative behavior, name the missing evidence and identify the operator who can verify it. Credentialed canaries use approved disposable targets and sanitized evidence. Production activation is a separate decision.

For review, cite the task and section, describe the failure or missing evidence, and suggest the acceptance check that would resolve it. Focus first on dependency order, ambiguous writes, stale ownership, active-job preservation, IAM boundaries and the exact meaning of each live gate.

## Decisions for review before enabling active execution

| Decision | Responsible task | Evidence that resolves it | Behavior while unresolved |
| --- | --- | --- | --- |
| How to recover known unfinished effects within a bounded read budget after restart, including rollback-era rows | [T10](T10.md), [T18](T18.md) | Durable discovery design and tests for large settled history, interrupted rebuild and delayed index | Block competing operations and report incomplete recovery. |
| Which state changes invalidate a pending dispatch and how the transition checks them | [T10](T10.md), [T13](T13.md), [T16](T16.md) | Revision and provider precondition checks with last-moment identity/job/target changes and lost responses | Keep the action advisory or unresolved. |
| How to authenticate bootstrap reports and isolate offers when job code can access task credentials | [T11](T11.md), [T14](T14.md), [T17](T17.md), [T24](T24.md), [T25](T25.md) | Reviewed protocol and live peer-denial tests using two executor identities per backend. Proof a job cannot forge terminal evidence or obtain plaintext JIT. | Treat process state as unknown and block unverified JIT or cleanup paths. |
| Which GitHub observations prove assignment accounting, closure and safe idle retirement | [T23](T23.md) | Target-specific ordered test results for exact request IDs, lost responses and dispatch-before-start | Keep reservations or sessions in recovery as applicable. Protect connected idle runners. |
| Which existing ASGs and Fargate task definitions can actually run one job safely | [T12](T12.md)–[T17](T17.md), [T24](T24.md), [T25](T25.md) | Owner-approved pinned launch configuration, per-backend live limits, role/identity and job evidence | Keep incompatible or unproven profiles report-only. |
| How each controller host proves fencing and rolls back without stale-owner writes | [T19](T19.md)–[T21](T21.md), [T26](T26.md) | Exact process/task evidence or dual-surface isolation, clock margin and interrupted-effect recovery | Successor stays in recovery and active jobs continue. |

Record acceptance at the right level. A source inspection establishes available APIs, a fixture establishes behavior against modeled inputs, synthesized templates establish intended resources and policies, and a live canary establishes the behavior of the named target. Include the checked commit, exact scenario and observed result. Link the sanitized run or artifact, state any untested variants and keep their rollout gate visible.

## Cross-task acceptance

| Requirement | Implementation evidence | Live evidence |
| --- | --- | --- |
| Independent hosting and runner backends, including EC2-only with no ECS resources | [Root composition and Docker](T19.md), [shared infrastructure](T18.md), [hosting](T20.md)–[T21.md) and [offline matrix](T22.md) | [Backend canaries](T24.md)–[T25.md) and [mixed operation](T26.md) |
| YAML-only addition and safe removal of a runner profile | [Catalog and retirement](T10.md), [permission preflight](T18.md) and [matrix](T22.md) | [Config and rollback exercise](T26.md) |
| No duplicate provision, registration or unsafe cleanup across restart | [Dispatch](T10.md), [JIT](T11.md), [ASG scaling](T13.md) and [ECS retry/cleanup](T16.md) | [GitHub contract](T23.md), [backend canaries](T24.md)–[T25.md) and [takeover](T26.md) |
| Enterprise PAT credentials never reach job runners | [Handoff boundary](T11.md), [bootstrap](T14.md) and [task bootstrap](T17.md), [active IAM](T18.md) | [PAT authentication and rotation](T23.md) and [runtime role isolation](T24.md)–[T25.md) |
| Dry-run inventories without mutation or production session interference | [Read adapters](T12.md) and [Fargate inspection](T15.md), [read-only role](T18.md) and [root composition](T19.md) | [Audit evidence](T26.md) |
| Existing legacy runners and active jobs survive controlled migration and rollback | [ASG preflight](T12.md), [drain](T10.md) and [host process](T19.md) | [ASG canary](T24.md), [rollback rehearsal](T26.md) and [migration sign-off](T27.md) |
