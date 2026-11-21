# Review triage

The supplied review was checked against main commit `3903e4039f9883330cefc4db8b84ffb577d46781`, which includes the executor ledger from PR #33. Issues #34–#48 track the confirmed defects individually.

| Fix group | Confirmed findings |
| --- | --- |
| Lifecycle and recovery | #34 unsafe assignment accounting, #35 acquisition index starvation, #36 erased drain proof, #37 pending-delivery retirement gate, #43 replay statistics timestamp, #44 dispatched admission on closure, #46 effect replay deadline |
| Listener and GitHub adapter | #38 lease renewal resilience, #39 rejected credential refresh, #40 absent creatable sibling, #41 interrupted close during shutdown, #42 sanitized absence errors, #45 retry attribution |
| Configuration identity and validation | #47 repository runner group, #48 runtime artifact digest |

## Findings with a different disposition

- **Missing DynamoDB transaction tokens is a false positive.** The pinned DynamoDB SDK v1.74.1 installs `OperationIdempotencyTokenAutoFill` in initialization and supplies a UUID provider by default. `Store.apply` leaves the pointer nil, allowing this middleware to populate it before the SDK retry loop. Adding another token generator would duplicate SDK functionality. This does not establish the outcome of a separate application call after all retries fail. Existing durable-state recovery remains necessary.
- **Immutable execution bindings are an intentional current restriction.** The catalog protects task-definition revision, bootstrap protocol, network, platform and scale-set identity. Relaxing these requires recording the previous configuration for existing allocations and verifying resolved identities when pinning a previously unpinned set. The review correctly identifies an operational limitation. T10b and the backend milestones must deliver that migration support before routine in-place execution changes are enabled.
- **Permanent ownership claims prevent automatic identity reuse.** Retiring a profile does not authorize moving its scale set to a different profile. A transfer needs an explicit migration with ownership proof. Automatic claim deletion would remove the safeguard against stale owners. The existing migration gate is retained.
- **Two separated retirement passes have no configured maximum gap.** The implementation requires the final pass to remain fresh and resets proof on adverse observations. A maximum first-pass age is an additional policy choice, beyond the current two-pass contract. Retirement still depends on authoritative current inventory, settled effects and closed sessions.
- **Processor timeout fields are private intentionally.** The public standalone entry point uses the lease deadline. The supervisor supplies operational timing for its managed lanes. External callers can bound their context. Public timeout knobs are an API enhancement.
- **The remote session recovery path, effect-history retention, ECS replay window and active-drain preflight are remaining implementation and live-validation gates.** They are already covered by T10, T16, T18 and T23. Distinct 404 errors enable future recovery decisions. A 404 alone does not establish a successful acknowledgement or safe session retirement.
- **Workspace housekeeping is independent of these code defects.** A user's main worktree and other worktrees can contain active work. The fixes use feature branches based on current remote main.

## Admission evidence limitation

Positive assigned counts cannot identify which reservations are included in a snapshot. Automatic assignment-based release is disabled until the GitHub event/statistics ordering contract is verified. Exact terminal events and explicitly verified request-specific evidence can release reservations. During this conservative mode, assigned work can consume both a reservation and a demand slot, reducing admission throughput. Separate statistics messages and batches larger than 96 follow the same rule. Accepted or uncertain reservations remain durable and visible through profile reads, outside the generic due-effects index. The lifecycle coordinator must reconcile unresolved requests with authoritative evidence.
