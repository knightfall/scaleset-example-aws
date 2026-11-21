# Controller task specifications

These files expand the remaining tasks in the [implementation plan](../implementation-plan.md) for engineering review. The repository baseline is `main` at `12c5f3b419bcbf1dc10646db9d4125a21f99ec56`, including [PR #56](https://github.com/knightfall/scaleset-example-aws/pull/56). Status describes code and fixtures at that baseline. Test commands specify future acceptance evidence.

The root CLI verifies configuration and produces offline dry-run reports. The lifecycle store, credentials, listeners and provider-neutral inventory coordinator have fixture coverage. Active runtime wiring, real provisioners, secure JIT delivery, deployment stacks and live canaries remain to be delivered. Existing synthesis covers an offline toolchain fixture. [Root entry point](../../main.go) · [CDK test](../../cdk/test/toolchain.test.ts)

| Task | Specification | Baseline state |
| --- | --- | --- |
| T10 | [Reconciliation, safe dispatch and retirement](T10.md) | In progress. Inventory, the unfinished-effect count and its bounded rebuild are merged. Active-only discovery is in [PR #57](https://github.com/knightfall/scaleset-example-aws/pull/57). |
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

Finish [T10c](T10.md#t10c-storage-and-recovery-contract) with active-only discovery and the durable request journal, then [T10d](T10.md#t10d-dispatch-and-admission) for the provider-neutral fake-provider dispatcher and [T10e](T10.md#t10e-shared-ownership-and-drain) for ownership and drain. [T11](T11.md#implementation-slices) splits reusable SDK/storage primitives from the handoff protocol. T12 and T15 provide read-only inventory against the T10d boundary for final runtime wiring and T05’s optional AWS inspection. T18 and T19 agree on runtime inputs before host binding. T18 consumes the T10e retirement and T11 IAM contracts. T23 can verify GitHub against an approved disposable executor once T09 and T11 are ready, independently of the full AWS and CDK matrix. T24–T27 use the approved backend and deployment integrations.

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

## Antigravity review disposition

This revision addresses F1–F8 from the review of `69e7868` in the specifications. D1–D9 below record design commitments, baseline progress and tests, with open service proof kept as a gate.

| Decision and finding | Specification change | Proof and unresolved gate |
| --- | --- | --- |
| D1 / F1 · Bounded discovery | [T10c](T10.md#t10c-storage-and-recovery-contract) records the merged checkpointed rebuild and defines the active index, count/revision transaction and writer continuity. [T18](T18.md) covers retention and restore. | Baseline fixtures cover interrupted rebuilds, 10,000 retained effects and startup of a rollout that retires a profile. Verify active discovery with an empty GSI and fair scheduling across profiles. Untrusted readiness closes mutation. |
| D2 / F2–F4 · Dispatch and admission | [T10d](T10.md#t10d-dispatch-and-admission), [T13](T13.md) and [T16](T16.md) use immutable nonsecret requests, revision-checked dispatch and evidence-specific outcomes. Admission credits only exact occupied-slot attribution. | Test changed evidence between plan/dispatch, readback ambiguity, definitive rejection and the cap-10 regression. The review's aggregate subtraction would allow two unsafe acquisitions in that case. |
| D3 / F5 · Handoff and exit | [T11](T11.md) specifies adapter, exact role/session key lookup, authenticated envelope and consume/revoke race. [T14](T14.md) and [T17](T17.md) separate EC2 isolated reporting from ECS task-terminal evidence. | Test cross-principal denial and workflow visibility with two real identities per backend. A task's `STOPPED` status is resource evidence. Job and registration state need independent proof. |
| D4 / F8 · GitHub service behavior | [T23](T23.md) maps early opt-in integration cases to PAT/App, assignment, drain, session recovery and connected-idle gates. | Require target-specific ordered results. Protect reservations, sessions and idle runners when the target cannot supply evidence. |
| D5 / F5–F6 · Backend eligibility and AWS API contracts | [T12](T12.md)–[T17](T17.md) specify operations, selected read clients, canonical identity, headroom, ASG cooldown and bounded ECS retry. | Keep the shorter correlation ID by convention. The current [RunTask](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_RunTask.html) reference gives a 128-character limit for `startedBy`. Confirm pinned wire behavior and actual role/network/scale-in evidence in T24/T25. |
| D6 / F3, F7 · Shared ownership and host fencing | [T10e](T10.md#t10e-shared-ownership-and-drain) owns renewal and quiescence. [T19](T19.md)–[T21](T21.md) define unique process ownership and host proof. | Exact prior-process/task proof, clock margin and outstanding-effect recovery before takeover. Successor remains recovering on ambiguity. |
| D7 / F7 · Startup and deployment | [T18](T18.md)–[T22](T22.md) pin table, asset, argument and probe contracts, stop budgets and offline/live preflight separation. | Exercise the built image and synth actual resources. Deny missing/mismatched artifacts and an offline fixture presented as live proof. |
| D8 / F3 · Drain closure | [T10e](T10.md#t10e-shared-ownership-and-drain) and [T18](T18.md) require a durable session closure hold, final strong checks and verified removal of permissions. | Test restart/rotation after closure, adverse evidence after the clear passes, and no reopen. Preserve permissions on any unresolved work. |
| D9 / F3 · Immutable binding migration | [T10e](T10.md#t10e-shared-ownership-and-drain) and [T27](T27.md) keep in-place rebinding blocked in v1 and document additive migration, handback and sign-off. | Exercise different claimed identities and reject accidental reuse. Same-identity transfer requires explicit owner proof. |

The [restore drill](T26.md) also carries the original plan's requirement to sanitize restored handoffs before restoring bootstrap access. [T24](T24.md) and [T25](T25.md) define the identity and workflow isolation evidence, and [T27](T27.md) supplies the readiness record.

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
