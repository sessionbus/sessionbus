# File-transfer implementation consequences

Implementation follows the independently cleared core contract: DRAFT-3
`77821bfb`, wire/budget packet `325918a8`, automatic-start addendum `2173da82`,
and aggregate-accounting addendum `a5a808e4`. Architecture accepted the 21 MiB
host / 28 MiB hub controlled-buffer targets, dispatch-time request IDs, and
8-control / 1-bulk fairness. These are engineering limits and measurement
objectives, not measured RSS, throughput, or product acceptance.

The feature adds offer and request workflows on one memory-only engine. Paths,
files and the direct filesystem implementation remain endpoint-local. A receiver's atomic
publication is the commit point. No terminal table, spool, reconnection replay,
or assumed rollback after publication is introduced. Owner m07n/teqn supersedes
the original no-grant requirement: the SDK default copies directly as the OS
user in every product/mode. There is no product permission mapping, native
command executor or permission-scope refusal; actual OS I/O errors remain.
Tightening transfer permissions later is OPEN and is not a release gate.

## Preservation and affected lifetimes

Ordinary peer/worker hello, message ownership, idle wake, busy delivery, trace,
Run admission/result/ACK, persistence and explicit close retain their existing
semantics. Old endpoints receive no new method/result without opt-in. New
endpoints connected to old daemons retain ordinary functionality and refuse
transfer actions. Both hub legs must negotiate the new ALPN superset; old
wake/trace/roster/no-ALPN fallbacks remain usable for ordinary traffic.

Only accepted streaming/committing transfers hold a lane. Pending/preparing
requests do not extend grace. Preparation reserves resource capacity separately
from lane holds. Both participant generations activate before the destination
is armed and the source worker starts automatically. A close that wins the
activation race refuses preparation; partial activation releases its original
reservations once. No model callback or extra consent is used to start bytes.

After acceptance the connection owner, not an individual tool-call context,
owns the transfer. A cancelled call or lost reply is not cancellation proof.
Explicit close, generation loss and expiry terminate the matching live transfer.
Same-ID replacement must not adopt old file handles, queued chunks or credit.
An unconfirmed commit is UNKNOWN remotely; a receiver with the publication fact
reports completed and never removes its committed destination.

## Boundary cases and evidence obligations

- Empty file uses the same prepare/start/finish path; pending state has no file
  handles. Populated state is bounded by pending/active/byte/descriptor caps.
- Concurrent duplicate live accept retains one binding and one writer. Changed
  paths, terminal/expired IDs, stale generations and malformed offsets fail
  without recreating or replaying work.
- Source identity/size changes fail; streaming hash proves received bytes, not
  an immutable filesystem snapshot. Same-size in-place changes can escape the
  final stat check. No new durability/fsync guarantee is implied.
- No-overwrite publication is atomic. Successful link followed by failed temp
  unlink remains committed with an explicit cleanup failure. Cancellation before
  publication joins the writer and cleans its temp; after commit it never
  retracts the destination.
- Disk-full/hash/size/credit failures release owned buffers, handles and holds
  exactly once; unknown active cleanup is reported instead of hidden.
- Control and machine chunks have separate bounded admission at reader/owner,
  correlation and writer seams. Bulk never calls overflow-closing control Send.
  IDs are assigned in selected wire order, not before priority reordering.
- A partial NDJSON write cannot be preempted. Message-latency objectives apply
  under the documented draining-link workload; no new short timeout or healthy
  connection-loss trade-off is silently added for a stalled socket.
- Multiple transfers/Run terminals choose one grace anchor. Status, ACK,
  collection and notice do not re-arm it; a real native Run induced by ordinary
  notice delivery still follows normal Run policy. Never-started, unavailable,
  refused Run, auto_close_ms=0 and persistent-owner-exit cases stay distinct.

Meaningful implementation evidence must cover old/new bootstrap pairs, both
hub legs, adverse frame/credit/queue limits, duplicate/cancel/commit/loss races,
continued messages and useful Run/recovery, and the specified 1 GiB workload.
Existing tests are preserved; no host install/restart, native model, live hub,
release or first-product claim follows an isolated core test. The independent
implementation review and configured CI/bot gates remain required.

The SDK's new single writer keeps ordinary calls/replies under their previous
blocking write backpressure when its control queue is saturated. Bulk has its
own bounded admission; its saturation does not invoke the existing control
outbox's overflow-close policy. The core connection uses a separate four-frame
bulk ingress channel. Request monotonicity is checked in wire order before
control/bulk scheduling, so prioritizing control cannot falsely reject an older
bulk ID. Outbound IDs are allocated at actual writer selection; an owner's
bounded dispatch correlation is installed before the response is handled.

Endpoint transfer opt-in is separate from public lane descriptions and native
permissions. Peer SDKs reserve the existing info key; workers send the optional
bit only with a versioned, full exact one-use launch token. Standard new SDK
Peer/Worker owners now enable this negotiation by default; old SDKs retain their
original non-opt-in behavior. Capabilities remain
bound to authenticated attachment generations. A capability change cancels its
old transfer generation without changing ordinary peer ownership semantics.

Isolated fixtures cover actual offer/request on one daemon and through a private
TLS hub, Go/JavaScript endpoint interoperability, atomic no-overwrite publication,
temp cleanup, source failures and subsequent ordinary list/send. They use only
exact test-owned paths and are not native-policy enforcement acceptance. The
review packet separately records race, capacity, codec and 1 GiB measurements;
no test count or admission budget is a production memory/permission guarantee.

Custom adapters can return ErrTransferCommitUnknown when publication's
answer is lost. This is distinct from confirmed non-publication and confirmed
publication (including post-publication cleanup failure). The SDK reports
UNKNOWN and does not run destructive Abort or retry publication; its unknown
cleanup remains charged until generation end. Native child file FDs count within
the same aggregate four; executor processes, control pipes and bounded JSON must
also have explicit adapter resource costs. Raw Connection brokers have an
exported observed-hello/routing hook with the same negotiated generation lifetime.

The approved machine-only `transfer.fail` addition distinguishes pre-finish
`source_changed`/`source_read_failed` from explicit cancellation. It validates
source role/current generation and uses the same serialized commit arbitration.
Transport loss is not an arbitrary source-failure reason. A late failure cannot
downgrade confirmed publication or remote UNKNOWN. Native error text stays local.

Resource retirement joins every admitted queued/in-write chunk and finish before
releasing its combined reservation. Passing one I/O-mutex barrier alone would
allow queued payload leases to outlive their accounting. Ended records refuse new
leases and are not exposed as stale streaming status while uncertain cleanup is
still charged. Peer reconnection does not wait for old native cleanup; endpoint
quotas cover overlapping generations and explicit executor disposal joins them.

Ordinary request correlation now includes queued and dispatched requests, with
owner-local lifecycle/response handling draining dispatch metadata before making
decisions. The existing delivery-capacity test counts queued plus dispatched
requests while keeping all record/cursor/order/capacity assertions. Deferred-close
tests retain their original assertions. No test is relaxed to accept lost work.

Transfer alphabet validation uses an exact ASCII recognizer for the schema's
base64 pattern and validates typed chunks without redundant JSON copies. The
schema language, canonical padding/size checks and ordinary validation remain
unchanged; exhaustive short-string comparison against the original regex is a
separate adverse control. This addresses measured codec CPU/allocation costs,
without a new parser, transport, runtime or dependency. JavaScript changes are
parity work in the already-public SDK; the daemon/hub remain native Go.

The canonical protocol section is added to UNIVERSAL-SESSION-PROTOCOL.md from
the architect-cleared contract and the generated bus protocol is regenerated
verbatim. Historical frozen design/evidence artifacts are not rewritten. Final
model-facing tool descriptions still require the owner's approval; no new tool
text or deployment authority is supplied by this core candidate. The former
native permission executor track is superseded, not an acceptance gate.

## Owner-selected direct filesystem default and same-review contract fixes

The default core implementation adds no runtime/dependency, process or extra
inbox/outbox file. It opens regular sources nonblocking before fstat, checks the
same handle after streaming, creates one random exclusive 0600 sibling temp and
publishes via atomic hard link (no-replace) or explicit atomic rename (overwrite).
Existing symlinks follow ordinary OS semantics; overwrite replaces the named
entry. Go binds the destination parent FD and uses existing x/sys Unix relative
operations. JavaScript uses public fs APIs and detects changed parent identity;
a later directory rename/replacement may leave an explicitly reported owned-temp
cleanup failure, never a silently successful cleanup. Default Go uses at most
three filesystem FDs for both roles, JavaScript at most three. Cancellation
closes owned handles/temp, never the published destination. Duplicate Commit
cannot republish, and UNKNOWN/custom adapter behavior remains unchanged.

DEV1 F1/F2 findings correct live duplicate accept and pending expiry. Concurrent
same-binding accepts share the first preparation/start/holds and return its live
descriptor; local path/overwrite conflicts, wrong generation/role and terminal
IDs still reject. The author's previous error-expecting assertion was wrong and
is replaced by sequential/concurrent adverse controls. Pending expiry emits the
expired terminal enum, retaining ordinary notices, no hold and exact cleanup.

DEV1 F3 corrects remote explicit-close and resettable deadline classification at
the same phase lock: precommit close is cancelled; pending deadline is expired;
preparing/streaming deadlines fail with their actual timeout reason; committing
remains UNKNOWN unless exact completed finish knowledge is already retained. A
confirmed receiver commit cannot be downgraded. Actual link/generation loss
remains distinct. The engine uses the same phase-sensitive timeout rule and now
classifies/assigns terminal knowledge under one record lock, avoiding a gap in
which finish could enter committing after nonpublication was inferred. Existing
resource joins and ordinary terminal/hold cleanup are preserved. Controlled
phase/race tests are not claimed as physical lifecycle measurements.

The previous 1GiB measurements remain exact e8df author evidence with controlled
executors; the default-FS delta uses real Go/JavaScript filesystem interoperability
and adverse filesystem tests, not a repeated or relabeled workload measurement.
