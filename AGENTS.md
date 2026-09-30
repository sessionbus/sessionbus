# Standing owner instructions

## Implementation language

Use Node.js only when a required capability cannot reasonably be implemented without it. Convenience, familiarity, or an available JavaScript SDK is not sufficient justification. Explain the concrete necessity and total runtime, dependency, resource and installation cost before adopting it. Prefer a small native implementation where feasible. This preference does not authorize discarding existing product knowledge, tested behavior or regression fixes when changing languages.

## Mandatory analysis before every change

Owner's verbatim instruction, 2026-09-30:

> ok, now, ANY change require extensive analysis of edge cases and consequences

> this is rule

> record also in AGENTS.md in ALL repos. Also add to it KISS, RCA etc.

This is a mandatory rule, not a preference. It applies to production code, tests, test oracles, configuration, installation, documentation and diagnostics, including changes described as small or defensive.

Before changing anything, record the intended behavior, the existing behavior being preserved, and the affected callers and lifecycle. Trace actual consequences through relevant code and evidence rather than assuming that a helper name, error label or passing test explains them. For each applicable case, analyze normal operation, empty and populated state, boundary races, cancellation, partial success, uncertain outcomes, duplicate/stale events, shutdown, reconnect and subsequent recovery. State why a case is inapplicable instead of silently omitting it.

For messaging changes, explicitly account for connection/presence, inbound and outbound tools, message ownership, never-attempted and attempted/uncertain entries, capacity/accounting, active delivery, idle wake, native session identity, permissions, terminal results and later valid delivery. A missed model-step callback does not itself justify disconnecting, discarding input or disabling future delivery. Still-unattempted input must remain eligible for a later valid step; uncertain native attempts must not be blindly replayed.

Describe user-visible consequences and possible functionality loss. Do not label shutdown, feature removal, delayed delivery or queue disposal a repair merely because it makes a failure explicit. Unresolved behavior changes must remain explicit; existing implementation authority is not authority to silently weaken the owner's requirements.

Obtain independent review of the consequence analysis before adopting the change. Review the actual implementation and meaningful regression evidence before merge/install. Tests must exercise adverse outcomes and continuity, not merely confirm the implementation's chosen behavior. Test-oracle edits require analysis of false PASS/FAIL cases and preservation of historical evidence. A receipt, source inspection or green build alone does not establish working product behavior.

This rule does not require repeated owner permission for already-authorized work. It requires the analysis and review to be performed, recorded and used to guide that work.

## KISS: the smallest sufficient change

Prefer the simplest design that satisfies the complete requirement and preserves tested behavior. Compare the proposed change with the existing supported native capability and the smallest local correction before adding abstractions, queues, state, transports, dependencies, processes or fallback paths. Explain why each addition is necessary and who owns its lifecycle. Fewer lines are not simpler if they hide uncertainty, lose functionality or shift complexity elsewhere. Do not rewrite or change languages by discarding established product knowledge and regression fixes.

## RCA: establish the cause before fixing it

For a defect, reproduce it where feasible and trace the concrete causal sequence through the actual implementation and relevant native version. Separate observed facts, hypotheses and unknowns. Compare against earlier working behavior and retained evidence; challenge claims that native support is absent before designing around them. A symptom, receipt, error label or coincident event is not a root cause. If reproduction is unavailable, state that limitation and what evidence supports the proposed cause. Fix the cause at the smallest appropriate boundary; do not mask it with blanket catches, disconnects, retries, feature removal or weakened tests.

## Preservation and proof

Derive acceptance from the owner's required behavior, not from what the current adapter happens to implement. Preserve existing regression tests, or map each changed test to an equally strong or stronger replacement and explain why. Include failure controls that demonstrate the test can reject the defect. Record exact source/build/configuration provenance and distinguish source findings, mocked tests, native admission and observed native effects. Unrun or ambiguous checks remain unrun or unknown; do not turn them into PASS.

Busy delivery and idle wake are separate requirements. A message delivered at a later model step of the same original active task is delayed active delivery, not automatically a delivery failure; record latency separately. Intentionally holding messages until the task ends does not satisfy active delivery. Retain unsent input for the next valid opportunity, preserve idle wake, and never replay an uncertain native handoff merely to obtain a clean result.

## Scope, coordination and truthful completion

Keep one coordinated writer per source or host change. Preserve other contributors' uncommitted work. Follow repository review and validation gates at the actual proposed commit, and report unresolved findings and behavioral trade-offs before adoption. Keep fixes focused; do not bundle unrelated cleanup or silently replace the native runtime, installation layout, permissions or identity model. Ordinary authorized work should continue without repeated permission requests, but a newly discovered loss of required functionality is not implicitly approved.

After each pushed correction, wait for the configured review bot to start and finish reviewing that exact head. Read the full review, including summary concerns, inline comments and nitpicks. Address every item by fixing it or recording an independently verified refutation and replying; an initial clean summary is not permission to ignore other findings. Merge only after independent review, completed bot review and required CI are clear at the head being merged. Pin the merge to that reviewed head; a mergeable branch or green build alone is insufficient.

Report what actually changed, what was verified, and what remains incomplete. Documentation, a review verdict, a successful build or a receipt cannot substitute for the product behavior the user requested.
