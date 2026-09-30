# Active-work messaging: original requirement and verbatim owner record

Recorded 2026-09-30. This records and restores an original requirement; it does
not introduce a new requirement or establish the date it was first given.
The owner explicitly requested that these statements be preserved verbatim in
the protocol documentation. Wording, spelling and emphasis below are unchanged.

## Normative requirement

All agent products must deliver incoming Sessionbus messages into ongoing work
at the next supported native input/steering boundary, before the original
long-running task or Run finishes. This applies to interactive sessions and
managed lanes. Consensus, corrections and collaboration must be possible during
work. Waiting for final idle or automatically starting another Run afterward
is insufficient. See [message.deliver in the protocol](../../bus/docs/PROTOCOL.md#messagedeliver)
for the normative requirement, truthful receipt boundaries and BUSY-MID evidence.

A native model response or tool operation need not be forcibly interrupted.
An adapter must use the earliest supported input boundary within the ongoing
work, rather than imposing a whole-task completion barrier. Where the native
product cannot do that, record a conformance gap; do not redefine the requirement
to match the implementation or claim that documentation alone fixes it.

## Verbatim owner statements

The following are consecutive relevant owner statements from the discussion;
intervening assistant/team messages are omitted. The first two are questions,
not approval of either delivery design.

> coming back to our original question. can you elaborate on how pi native clear queue or what ever it is called works?

> why Follow-up and not Steering ?

> I mean, for all of products, if product does tunrs for an hour, we dont want our message to be hold till the end of it

> this must be true for all products!

> oh fuck! this was requirement from very beginning! how they fuck and when it changed?!

> isn't that fucking obvious, that you can not have fucking consensus work if messages are hold till the end of work?!

> record all of this shit in some verbatim. as part of protocol docs, whatever! I am fucking tired to state  the same again and again for month already, and you destroying working products again and again

## Recorded failure: requirement weakened to fit adapters

The assistant incorrectly called the owner's restatement a "clarification" and
treated it as a new requirement. That framing is withdrawn. The owner did not
approve a relaxation in these statements.

The following incompatible acceptance language was found in the September 29
product test matrix r3.7 (preserved separately as historical evidence):

- Interactive I1.4s: "same-run inclusion or an automatic next turn, per the §7 carrier".
- Lane M1.5: "queued carriers: an automatic FIFO next run consumes `BUSY` (s, c), and C-X then collects and acks that run."

Those alternatives allowed tests to pass according to an adapter's existing
carrier instead of requiring delivery during active work. They are superseded
as sufficient busy-message acceptance. A historical PASS may still establish
admission, eventual consumption or a particular regression fix; it does not
establish BUSY-MID unless its evidence actually shows mid-task consumption and
reaction before the original terminal. Preserve the evidence and narrow the
claim rather than deleting or rewriting the historical result.

The published protocol at core commit `b4855293` also contained this exception:

> Delivery is mandatory. Products that cannot inject during a native run queue
> for the next turn in their wrapper and report that disposition; the daemon
> has no injection capability flag or queue.

Its lane delivery section also permitted `not_running` when a product had no
source-proven automatic active-delivery path, with queued work started after
`turn.ready`. The wire fallback's existence was allowed to stand in for the
product requirement. The protocol now distinguishes valid completion-race
recovery from a still-busy adapter's failure to deliver into current work.

The quoted wrapper-queue exception is already present in this repository's
initial commit `707d9a4015d6cafbfd8835f04ad5e29d686c2e62`, dated 2026-09-08.
That is the earliest occurrence verified in this repository history, not proof
of when the owner requirement was first weakened or who made that decision.
The exact earlier provenance and the first weakened matrix revision remain
under investigation. No owner waiver is established by these artifacts.

## Rules for implementation, review and release

- Compare the implementation with this requirement; do not derive the required
  behavior from the implementation's current queue or native API name.
- "Turn" must identify the boundary in question: one model response/tool step,
  a whole native agent run, or a Sessionbus Run. Do not use that ambiguity to
  substitute after-task delivery for delivery during work.
- Test each product and mode with an actually active multi-step task. Observe
  a unique incoming message being consumed and reacted to before the task ends.
  A send receipt, queue length, or final-idle reply is not sufficient evidence.
- Keep delivery timing separate from receipt truthfulness, clear-queue loss,
  interrupt semantics, terminal correlation, FIFO/capacity and no-replay.
  Correcting one property must preserve the others and their regression tests.
- Do not discard working native mechanisms or tested product knowledge during
  a refactor. A simpler design must preserve the required behavior, not merely
  reduce code while silently replacing active delivery with deferred delivery.
- A native limitation is a gap requiring correction or an explicit owner
  decision. Review completion, green CI and generic merge/release approval do
  not authorize weakening the contract. Any proposed relaxation must state
  the lost behavior before that decision.
- Documentation and this record do not prove current product conformance.
  Reassess existing evidence, mark missing coverage unverified, identify real
  violations separately, and fix them without falsely declaring every product
  broken or every historical test useless.
