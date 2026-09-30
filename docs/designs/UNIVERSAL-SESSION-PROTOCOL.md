# Sessionbus Universal Session Protocol

Status: design in progress. Section 1 is the proposed wire contract; later
sections will derive the daemon, native product kits, DSH integration, wrappers,
and migration from it.

> **2026-09-30 owner-directed requirement restoration:** active-work delivery
> is mandatory for every agent product and mode. This restores the original
> requirement after weaker adapter and acceptance language was found; it is
> not a new policy. See the [verbatim owner record](../requirements/ACTIVE-WORK-MESSAGING.md).
> This documentation correction does not certify current implementations.

## 1. Wire

### 1.1 Roles and connection model

A session is one JSON-RPC 2.0 connection. A peer is a session opened by a
product. A lane is a session spawned by the daemon with a one-use launch token
and therefore also accepts daemon-directed session and turn methods. The same
connection carries presence, tools, message delivery, lane control, and turn
results. There is no child presence connection and no relay connection to the
daemon.

Each frame is one UTF-8 JSON object followed by a newline and is at most 1 MiB;
an oversized frame is closed silently because no request ID can be assumed.
Request IDs are integers in `[1, 2^53-1]`, with each direction counting from
one in its independent ID space; gaps are allowed, and only range and
per-direction uniqueness matter. When JSON repeats a member, the last value
wins on both Go and JavaScript implementations. Notifications, batches,
unknown fields and explicit nulls are invalid. The first request is
`session.hello`. A worker sends it exactly once. A live peer may re-hello under
the identity-transition rule below. The daemon admits no other request from a spawned
worker until `session.open` has returned its session ID and that ID is durably
committed. Request admission captures the source identity and groups from the
exact current connection once; later identity changes cannot rewrite authority
for a request already admitted.

A request with a usable ID whose params or method are invalid receives its
correlated `invalid_frame` response, or `invalid_hello` for `session.hello`,
before the reader closes the connection. Without a usable ID the reader closes
without writing. The reader never dispatches a later frame after either case.
Cancellation retains the same outstanding correlation until its reply or connection
closure, with its observer and caller result target disabled. Late replies are
validated and drained. A reply already claimed by the reader wins cancellation.
Already-cancelled calls submit nothing. Drains count toward the existing
256-operation bound; no polling or eviction creates room.
A response whose ID does not match an outstanding call is also an invalid frame:
the receiver closes the connection and fails its pending calls once.

> **0.5.0 scope — local Unix-socket TLS is specified but NOT implemented.**
> `SESSIONBUS_LOCAL_KEY` is reserved; both kits consume and scrub it and reject
> a nonempty value. The implemented local transport boundary is filesystem
> ownership and modes: an owner-checked `0700` directory and `0600` socket.
> Host-to-hub TLS remains implemented and required. See the worker-kit
> environment boundary below. This amendment is co-signed by fable-architect
> and astra-architect.

The reserved local Unix-socket TLS extension specifies that, when the daemon's
optional `local_key` is configured, every peer and worker connection instead
uses TLS 1.3 on that same socket. The R1 key derivation and public-key pinning
apply with fixed labels `client` for the connecting side and `daemon` for the
listener; one shared key replaces federation's SNI lookup. A keyed daemon never
accepts plain frames. A kit without the required key reports `daemon requires
local key`; a keyed client connecting to a plain daemon reports the TLS
handshake failure. The key is never a wire field, argument, or log value.
Clients read the daemon Unix-socket path from `SESSIONBUS_SOCKET` first. When
it is absent, the path is `$XDG_RUNTIME_DIR/sessionbus/presence.sock`, or the
short `/tmp/sessionbus-<uid>/presence.sock` fallback when the runtime variable
is absent (including launchd on Darwin). The selected directory is absolute,
current-user-owned, and mode `0700`; sockets are mode `0600`. Startup rejects
a presence path or sibling `lanes/<32hex>.sock` path beyond the platform's Unix
socket usable limit (107 bytes on Linux, 103 on Darwin), naming the limit and
path. Lane basenames are fixed at 32 hex characters by hashing the qualified
session ID, so product and federated identifier length cannot change this bound.
Durable tables remain in the XDG state root. Every spawned lane receives the
selected socket alongside `SESSIONBUS_LAUNCH_TOKEN` and, when configured,
`SESSIONBUS_LOCAL_KEY`.

A product is a binary; started with a launch token in its environment it is a
lane worker, with no mode argument. Lane-only workers such as non-AI tools are
never started without a token, so a worker flag would be dead syntax for them.
A flag plus a token would also create two sources of truth and require errors
for both disagreement cases; the token alone leaves one fact and zero
consistency checks. Per-spawn tokens are single-use and expiring, so an
ordinary human shell never contains one.

Every session has one canonical ID `id@host` and, when named, one canonical
name `name@host`; every output uses those forms on the session's own daemon and on
every federated host. The product owns the ID part because it owns the session
primitive that can address it; the daemon mints no session IDs. A caller may
use a bare ID or bare name as shorthand for its own daemon's host.
Resolution splits a qualified name on its last `@`. Name parts are 1–128
printable Unicode characters, including spaces, with control characters excluded;
`/` and `@` are allowed. Name values are preserved exactly: no trimming,
whitespace collapse, tokenization on whitespace, or Unicode normalization. The
128-character limit counts Unicode code points. ID parts remain 1–128 printable
characters with no whitespace or control character. The last `@` is always the
canonical host boundary. Host parts match
`^[a-z0-9][a-z0-9-]{0,31}$`. For identity input, no `@` means a bare local part:
the daemon appends the caller's host, then tries exact ID before exact name. An
input containing `@` is always split at the last one, and an unknown right part
is `unknown_host` rather than a bare name. The schema carries the printable-name
pattern and code-point bounds; the daemon separately checks name, ID, product,
and host grammar so changing one does not loosen another. Overlong or
control-bearing product titles are rejected visibly and are never rewritten.
The wire has no generic tool frame: after hello, a peer or committed worker
originates the ordinary client-to-daemon methods in this section. Product-facing
start/wait/status/interrupt/list/send tools are caller-kit sugar over them.

Turn input and result strings, `message.send.message`, and
`message.deliver.body` are each limited to 262,144 decoded characters through
the closed generated types. The raw 1 MiB framing guard remains an earlier,
independent byte limit because UTF-8 width and JSON escaping are not character
counts. Before retention, the worker validates native results against the schema
and the full reply envelope using the largest permitted request ID. Invalid or
oversized output becomes `unavailable`; the kit neither fabricates a failed native
terminal nor truncates a successful answer. Product-reported `truncated:true`
remains permitted when it describes native output. Both kits emit compact JSON.

The closed parameter and result shapes are authoritative in
`bus/sdk/go/protocol/session.schema.json`, exported as bytes by each public SDK.
An implementation validates each frame against that schema with a small
interpreter and no validation library, then decodes it into the closed types.
Unknown fields, missing or null required fields, and out-of-range values are
rejected before product code runs. The shared fixture file tests the same
definitions in Go and JavaScript. Optional product discovery is list data, never
launch authority. The same schema defines independent lane policy and stable
run collection within its existing **440-line cap**.

Authorization is visibility: a session may start, read, acknowledge, interrupt,
close, or message a lane exactly when it can see it through a shared group in
`session.list`. Lifetime ownership is separate: it determines cleanup on owner
exit, never an additional per-caller access restriction.
Peer identity and groups are asserted rather than attested on the trusted local
socket, and federation trusts the remote daemon's assertions. No peer
credentials, signatures, or other security machinery belong in this protocol.

There are eighteen methods.

#### `session.hello`

The session sends `session.hello` first. The request is one closed union. A peer
supplies its product-native bare `session_id` and an optional unqualified `name` part, plus
`groups` and `info`; the name part may itself contain `@` or `/`. The daemon
qualifies the ID and any present name with its effective host before installing the peer. A worker instead supplies a
one-use `launch_token`, its supported non-identity open fields, its ordered
extra-argument descriptions, optionally the product version, and optional
`supports_message_run`. Every managed worker must advertise this capability;
the daemon rejects spawning a worker without it before native Open. The branches are mutually
exclusive: a request with both discriminants or neither is invalid. A worker
sends hello only after its product and plugin are app-ready, so hello success is
the sole readiness fact. A peer ID matching a durable lane row is invalid. A
worker's product must equal the product recorded by its launch-token
reservation. The result is `{}`.

A peer publishes its authoritative native session ID, launch groups and product information once known. The native name is optional. Omit name when the product has not reported one; an empty string or generated stand-in is not a name. An unnamed peer is live and addressable by its session ID and normal groups. Name lookup does not match unnamed peers. A subsequent native title report updates the same peer identity using the existing rehello operation. Absence of a title field on an unrelated product event is not a title-removal assertion.

Name absence survives summaries, delivery-source metadata, and federation; it
is never qualified into `@host`. Go `PeerIdentity.Name` uses the empty string
in memory for absence and omits it on the wire. Go `Rehello(ctx, "", info)`
and JavaScript `rehello(signal, undefined, info)` explicitly remove a name.
Every rehello is a complete identity assertion: an omitted name means no name
now. The adapter sends that assertion only when the product reports removal.
An explicit empty string on the wire is invalid. Lane names remain required
and follow the existing daemon naming and uniqueness contract.

A live peer may send another hello. With the same `session_id`, it updates the
product-owned name and `info` in place; `groups` must equal the original
declared slice exactly, including order, or the daemon returns `invalid_hello`
and closes the connection. With a
different `session_id`, identity replacement is atomic: the old transient
entry and private group are removed, its reply sinks are detached, and pending
inbound deliveries fail once before the new hello is acknowledged; the new
identity is then installed with its own private group and the new hello's
groups. Requests already admitted retain their captured old source. This
same-connection transition sends no `session.superseded`. A worker never sends
a second hello.

#### `session.superseded`

The daemon sends the displaced connection the ID-bearing request
`session.superseded` with `{}` when a new peer connection claims the same
canonical identity. The displaced client marks that identity terminal before its
best-effort `{}` response, closes, and never reconnects that identity. The new
connection is already current, so every request from the displaced connection,
including another hello, is rejected. After the directory replaces the exact
entry and releases its mutex, the old connection's owner writes the supersession request once with
`supersedeWriteBound = 1s`, closes the old local socket, and never waits for an
acknowledgement. A writable socket returns immediately; the bound only stops a
dead client from blocking this path. If a displaced client races one final reconnect, that new claim
may cause one more swap; because each displaced instance becomes terminal, the
race is bounded and cannot flap indefinitely.

#### `session.list`

A connected session sends `session.list` with an optional `session_id` filter.
The result contains the matching visible sessions, or all visible sessions when
the filter is absent. Each item reports canonical `id@host` and, when named, `name@host`, whether its one
connection is open, and whether one run admission is active. Lane summaries also include normalized
`policy`; peers have no lane policy. The host is already carried by both canonical identities, so no
separate summary host field exists. This single method
replaces peer listing, lane listing, and lane status. Its optional `hosts` array advertises product names by host. Whenever
the local optional non-empty product list is configured, the daemon's effective
local host identity is present; an empty list is treated as absent. Federated
hosts contribute their published lists.
Advertisement never gates launch: the service PATH remains authoritative.

Every successful list also returns `self_info`: the originating caller's canonical
`session_id`, optional canonical `name`, `product`, and `groups`, captured when the
request is admitted. It is independent of the selected rows: a host or session
filter can exclude the caller without removing `self_info`. Federated lists keep
the originating caller, including directed queries to another host. This is the
same public identity used for message sources; credentials and owner tokens are
never included. Use `self_info.session_id` to recognize self, not a name or list
position. An unfiltered list includes the connected caller.

Updated SDKs accept an absent `self_info` from older daemons; that means identity
is unavailable in this response and must not be guessed. Previous SDKs reject
unknown response fields, so upgrade clients before deploying a daemon that emits
`self_info`.

Lane-row identity is immutable. Peers have no durable rows and follow the
re-hello and connection-supersession rules above. A session outside the caller's visibility is indistinguishable
from a missing session and yields `unknown_session`. Federation forwards
canonical identities unchanged; receiving daemons never relabel them. Host
qualification is a daemon invariant rather than a JSON-schema constraint.

#### `message.send`

A connected session sends `message.send` to exactly one `target`, one explicit
`targets` list, or one `group`. The daemon resolves recipients from current
connections, sends each one `message.deliver`, and returns one truthful receipt
per attempted delivery. The request retains one message body; explicit
multicast and group expansion do not create another protocol method.
Resolution validates the target name-part grammar, then tries exact canonical
session ID and canonical visible name. Bare input is first qualified with the
caller's own host. Each label yields its own receipt: an unknown session,
unknown host, or ambiguous name is rejected with that reason, while every
resolvable target is delivered. Resolved recipients are deduplicated by session
ID. Deliveries run concurrently, and one response returns their receipts in
label order after deduplication. There is no multicast timeout.

#### `message.deliver`

The daemon sends `message.deliver` to the target session with the message ID,
authoritative canonical `id@host`, optional `name@host` source identity, and body.
Every product implements delivery while idle and while a turn is running.
Delivery is a request for work: an idle agent must start a native turn without
a human prompt or a separate `run`. Active delivery joins native processing;
an input crossing the active-to-idle boundary must still be processed
automatically. Admission refusal, capacity failure, and uncertain transport
must remain explicit; none may be disguised as successful passive storage.

**Required active-work delivery.** This is the original product requirement,
not an optional adapter policy: every agent product, in both interactive and
lane modes, MUST make a message arriving during ongoing work available to that
work at the next supported native input/steering boundary. The adapter MUST NOT
hold it until the entire task, agent run, or lane Run reaches its final terminal
or becomes idle. An hour-long task must be able to receive corrections and
coordinate with peers while it is still working. A native processing boundary
may occur between model responses or tool steps; this does not require interrupting
an in-flight model response or tool call instantaneously.

A receipt describes admission evidence, not an exemption from this scheduling
requirement. In particular, `queued_for_next_turn` MUST NOT be interpreted as
permission to defer every busy message until after the whole current task.
Products with no suitable native mechanism have a conformance gap to resolve
or report explicitly; documenting eventual delivery does not make them conforming.
Idle wake and recovery of an input that actually crosses completion remain
required, but do not replace delivery during active work. Queue ownership,
interruption loss, capacity, identity and no-replay remain separate obligations.
Forced abort/restart or replay of an uncertain submission is not a substitute
for active-work delivery.

**Acceptance (BUSY-MID).** On each product's interactive and lane surfaces,
start a multi-step task, send a unique message while that task remains active,
and observe the agent consume and react to it before the original task's final
terminal. Record the native identity, message, active-work evidence, reaction,
and terminal ordering. A transport receipt, retained queue item, reply in a
later Run, or automatic turn after final idle is insufficient. Existing evidence
may be reused only if it demonstrates this ordering; otherwise the property is
unverified. Retain historical tests and fixes, but do not carry their PASS labels
from a weaker acceptance criterion.

The [owner's verbatim requirement and the recorded acceptance failure](../../docs/requirements/ACTIVE-WORK-MESSAGING.md) are part of this requirement's
provenance. Adapter limitations, receipt terminology, test fixtures and release
reviews must not silently weaken it. A proposed relaxation must identify the
behavior lost and obtain an explicit owner decision; a generic merge/release
approval is not such a decision.

Its result is exactly one closed receipt: `written`, `injected`,
`queued_for_next_turn`, or `rejected` with a nonempty reason.

> `written` means the integration completed one local transport write of the complete frame addressed to the native session captured for that delivery, using that session's product-owned carrier, with no explicit transport/native error observed before the result was emitted. It acknowledges only the local write. It does not assert that the native process parsed the frame, accepted its session ID, scheduled or retained the message, presented it, or consumed it. A native EOF without response bytes adds no acknowledgment. Absence of an observed rejection is not proof of acceptance.
>
> `injected` remains reserved for an identity-bound native admission acknowledgment. `queued_for_next_turn` acknowledges input retained for automatic processing. For peers this requires demonstrated native scheduling; for lanes it can also acknowledge the daemon's bounded queue after a definite pre-submission refusal. Neither means native admission, durability, or proof of model consumption. It must not mean waiting for another human prompt or explicit Run. Products must preserve the idle-wake and active-to-idle handoff requirement. Legacy adapters that only retain input passively are not conforming. A completed local write alone qualifies only as `written`; `accepted` is reserved for an explicit retention undertaking and is not an alias of `written`.
>
> The Claude interactive integration returns `written` at that local completion boundary. It preserves the captured native session ID and frame contents across asynchronous work; a later identity report never retargets the frame. A native-adapter `rejected` result requires an observed native refusal or a failure before any native submission, with a reason that identifies that boundary. An uncertain write or post-submission transport loss must not be represented as a native refusal or proof of non-consumption.

For every non-rejected sender receipt, `session_id` and `delivery_id` are
required and `reason` is absent. For an uncertain submission, the native
callback returns/throws the existing public `ProtocolError` with code `-32603`
(`internal`) and a diagnostic string in `data` identifying that uncertainty.
Both Peer and Worker kits preserve that RPC error instead of converting it to
a native-adapter rejection. The daemon reports its existing `rejected/no_receipt`:
no native receipt was obtained, and whether the product acted is unknown.
When the destination daemon rejects a delivery before enqueueing it because the
selected target has no current connection, it instead reports
`rejected/not_submitted`. That reason proves only this delivery attempt was not
dispatched; it says nothing about an earlier attempt or a future connection.
A connection lost after dispatch, a stale response, or an uncertain native
admission still reports `no_receipt`. A remote destination can return the same
proven `not_submitted` receipt; loss of the federation response cannot establish
that fact. Older daemons may still use `no_receipt` for an offline target, so
callers must not infer non-submission from connection state alone.
A completed write followed by native EOF without response bytes may report
`written`; EOF itself adds no acknowledgment. Partial or uncertain completion
must use the error path, not `written`. Neither path retries or replays.

BN01/BD01 require a coordinated upgrade: install the compatible daemon, both
public kits, and all federated/caller validators before enabling unnamed peers
or `written` callbacks. This amendment keeps protocol number 1; the closed old
validators are incompatible. Package previews are pinned to the reviewed PR
commit; a compatible published kit must get a new package version before
consumer release. There is no negotiation or fallback to `injected`/`accepted`.

#### `lane.describe`

A session sends `lane.describe` with a product token and optional `host`.
Absent `host`, or `host` equal to the local daemon, runs locally; another
connected host forwards the identical request one hop and performs the complete
probe there. The authoritative daemon starts `<product>` from its service PATH
with empty argv and a rowless one-use launch token in its environment, consumes its worker hello, returns the declared open
fields, extra arguments, and optional product version, then closes it without
sending `session.open`. The hello is emitted only after app-ready. Exit before
hello fails with the exit code and bounded trailing stderr; there is no
readiness object or readiness phase.

#### `lane.spawn`

A session sends `lane.spawn` either with a caller-chosen name leaf, product, open
options, optional `extra_groups`, and optional `host` for a new lane, or with
`resume_session_id` for a durable offline lane. Both forms accept the independent
policy fields below and optional live `trace` mode `off`, `events`, or
`content`; native open options cannot be overridden on resume. A peer's private group is
`session:<id@host>`. A lane's private group is `<parent private group>/<leaf>`;
its default groups are exactly its parent's private group and that new private
group, plus `extra_groups`. The parent's other memberships are not inherited,
and the daemon permits any explicit extra group without checking the parent's
membership. The daemon composes the lane's canonical name as `<parent name
part>/<leaf>@<target host>`. Nested spawns extend both paths: peer `pA@pdev`
with ID `u@pdev` has private group `session:u@pdev`; after it spawns `pD` and
that lane spawns `pE`, the grandchild is named `pA/pD/pE@host` and has private
group `session:u@pdev/pD/pE`. Each level sees exactly one level down by
default; granting the grandparent's private group or a shared group through
`extra_groups` widens visibility explicitly. The leaf itself may contain `/` or
`@`, and the last `@` remains the host boundary. A composed name
part beyond 128 characters is invalid request data. Absent `host`, or `host` equal to the local daemon, spawns locally;
another connected host forwards the identical request one hop and performs the
entire transaction there. The row exists only on that authoritative target.
Resume never carries `host`: its session ID already selects the host. The
authoritative daemon resolves and starts the worker, waits for hello, sends
`session.open`, durably
commits the product-returned session ID, and only then returns `{session_id,policy}`. The row
stores the original closed open-options object as one JSON value. Resume replays
that stored value unchanged, preserving `arguments` order, with
`resume_session_id`; policy selection is separate from immutable native open options. The one
spawn/open transaction timeout covers all of those steps.
Unsupported supplied
open fields, an invalid session ID, exit, or timeout fail truthfully and do not
publish a live session.

One lane owner processes the open result, exit, timeout, and shutdown in event
order, so exactly one outcome finishes the request. A fresh spawn has no ID, so
its composed name is reserved until the product returns an ID. The launch
token, not a speculative ID, keys the provisional worker until open returns.
Resume requires the returned ID
to equal `resume_session_id`. A fresh open returning an ID already held by an
existing row fails with `spawn_failed` and the exact text `session id already
exists`. The lane owner commits while handling the open response, before it
handles the next inbox frame. An exit drains the closed socket first, so an
already-written valid open response can commit; EOF first fails the spawn.

The normalized `policy` contains `persistent`, `auto_close_ms`,
`notify`, and, when applicable, `owner_session_id` or `notify_target`.
Fresh defaults are `persistent:false` and `auto_close_ms:60000`.
Idle wake is mandatory, not a policy choice. Zero disables auto-close; positive values are milliseconds
up to 9223372036854. Persistence controls owner-exit cleanup independently of
terminal auto-close. All four persistence/auto-close
combinations are supported. Open performs no input and arms no deadline.

On resume, persistence survives and may be promoted, never demoted. A
nonpersistent lane acquires the resuming owner. Omitted auto-close resets to
60000; custom or disabled grace must be supplied again. Parent-owned lanes notify their current owner by default;
`notify:false` disables that delivery. An explicit `notify_target` may name that
same owner; a different target is rejected.
Fresh persistent lanes have no implicit target: `notify_target` enables it,
`notify:true` requires a target, and `notify:false` clears it. A simultaneous
false and target is invalid. Persistent resume preserves an omitted target;
promotion preserves the prior enabled owner destination as its explicit target.

Completion pointers are messages and wake idle recipients. To bound automatic
notification chains, a run seeded by a completion pointer emits no automatic
completion pointer of its own. Its output is still retained and collectable,
including by a third-party owner who consequently receives no automatic notice
for that run. Explicit sends made by the woken product remain ordinary messages.
A trace-seeded run keeps the existing completion behavior.

Completion origin is daemon-owned metadata, absent from public message params.
The authenticated federation envelope carries `completion:true` only for a
single-target generated pointer. It survives delivery hold/reseed, but is not
inherited by the product's explicit sends. Updated links negotiate TLS ALPN
`sessionbus-wake/1`, which also supports trace and roster. Across an older link,
a marked completion forward returns `forward_lost` without dropping the marker
or closing healthy federation; ordinary sends continue. Deploy the matching hub
and host daemons to preserve cross-host completion notifications.

The authenticated spawning caller owns nonpersistent lifetime. Same-ID peer
supersession atomically transfers that lifetime before retiring the old
attachment. Name updates preserve it; different-ID replacement or actual detach
ends it. A subsequent reconnect cannot revive cleanup already admitted.
Publication rechecks owner lifetime, including loss during provisional Open.
The worker launch-token claimant and visibility groups are not lifetime owners.

#### `trace.configure`

A parent sends `trace.configure {session_id,mode}` with mode `off`, `events`,
or `content` for one direct child. The result is the effective
`{session_id,mode}` at the live observation boundary. Only the daemon-validated
live parent relationship grants this authority; visibility, group membership,
or a claimed session ID does not. The setting defaults to `off`, is held only
in memory, and is neither `LanePolicy` nor durable row state. An omitted
`lane.spawn.trace`, including on resume, selects `off`; a former parent's
setting is never inherited.

The initial `events` scope covers Sessionbus message-send and settled-delivery
metadata. `content` additionally includes the message body. Run/lane lifecycle
events and native prompt or result content are outside this slice. A parent copy
uses ordinary `message.send` / `message.deliver` body delivery, with no new
delivery fields or event transport. Copies are live and best-effort: there is no
trace persistence, replay, catch-up, or recovery after restart.

This remains protocol 1, but the method and spawn field extend closed request
schemas. Daemon, SDK validators, and tool declarations therefore require a
coordinated upgrade. Older daemons reject unsupported trace requests through
their existing closed decoder; a trace-aware daemon returns `unsupported_trace`
when an involved federated host or hub cannot carry the requested control.
Callers must not interpret either failure as enabled tracing. Upgrade every
daemon and hub involved before requesting tracing again.

#### `session.open`

The daemon sends `session.open` only to a token-authenticated worker. It carries
always-present canonical composed `name` and `groups`, optional
`resume_session_id`, and one closed `open` object containing only the
non-identity options accepted by `lane.spawn`, plus normalized `policy` outside
that native options object. Values of `permission_mode`,
`model`, and `reasoning_effort` are product-native strings passed through
verbatim: the daemon checks only shape and declared field support, while the
worker rejects an unsupported value as `spawn_failed` with
`stderr_tail:["unsupported value <field>=<value>"]`. `arguments` is an ordered
array passed verbatim to the product integration. Hello's `extra_arguments`
entries document those strings for callers; the daemon never interprets or
enforces them. The worker applies `name` as the product's session title wherever
the product exposes a title primitive, creates or resumes the native session,
and returns `{session_id}`. A successful result is
the commit point that turns the provisional worker connection into lane
presence. A probe never receives this method.

#### `turn.start`, `turn.run`, and `turn.execute`

A caller sends `turn.start {session_id,input}`. The daemon reserves its one run
slot and sends `turn.execute {session_id,run_id,input}` to the resident worker.
The worker reserves capacity before native dispatch and acknowledges a canonical
`RunRef` `{session_id,run_id}`. This is worker acceptance, not native message
admission or a terminal. `turn.run` has the same caller input and composes that
admission with a non-consuming terminal wait, returning `RunStatus` below.
Neither operation consumes the result. New admission cancels the previous
terminal deadline; a proven pre-admission refusal restores the exact prior
deadline and reuses its provisional sequence. Receipt failure after admission
never rolls back the run.

Run IDs are an opaque worker-lifetime generation plus contiguous positive
sequence (`generation/1`, `generation/2`, ...), allocated by the daemon and
validated by the kit before native dispatch. Only refused provisional IDs may
be reused. Returned session IDs remain canonical across federation. Resume
starts a new generation and does not recover answers from the retired worker.

For every idle lane delivery, the daemon reserves the ordinary run slot
and adds its private `run_id` to the original `message.deliver`. There is no
passive delivery mode and no additional caller command is required. The Worker
invokes `RunInput` with exactly one text input or full delivery seed. The seed
preserves original message ID, source and body; its private transport run ID may
be removed before the callback. `Run.ReportDelivery` answers that original RPC
once with the observed native receipt and may block on its transport write.
Adapters must call it outside native reader locks. Missing or uncertain receipt
uses Internal/no_receipt, not a guessed refusal. Invalid receipt encoding or
write failure closes the connection so the original request cannot hang.
Active delivery keeps the ordinary native admission path and Run token and
follows the required active-work delivery contract above. An automatic later
Run is not a substitute for consumption during ongoing work.
At the final native handoff, the product synchronizes delivery with native turn
completion. If the turn ended and nothing was written, steered or queued, it
returns `ProtocolError(-32004, not_running)`. This refusal must precede any
native submission. The kit also returns this error
without invoking the product when the run is already absent or its context is
cancelled. The product check is still required: completion may race the kit's
check. Never return this error after native admission or an uncertain write.

**Current implementation gap.** Existing adapters may also return `not_running`
while native work is still active because they lack an active-delivery path.
The daemon fallback described below preserves automatic eventual processing;
it does not satisfy active-work delivery in that case. That adapter behavior
requires correction. The valid completion-race fallback and its no-replay
protections must be preserved when correcting it.

For this pre-submission refusal on an ordinary lane delivery, if ready already
arrived, the daemon immediately admits the message as new work and returns its
native receipt. Otherwise it reserves one future worker record, retains the
message under its existing 256-call bound, and immediately answers the sender
`queued_for_next_turn`. It must not hold the sender RPC until the recipient's
turn ends: two active agents awaiting each other's send would deadlock.

After the current `turn.ready` acknowledgment, the daemon automatically starts
one retained message as a new Run; subsequent retained messages start FIFO after
each preceding Run ends. No human prompt, public Run, or sender retry is needed.
Message ID, source, body and daemon origin markers are preserved. Future record
reservations count alongside all unacknowledged Run records against the worker's
256-record bound; completion does not free a record. Successful matching-worker
`turn.ack` releases capacity. A full queue or record budget returns Busy before
queue acknowledgment, and explicit starts cannot consume reserved slots.

Queue admission is in-memory, not durable or a native receipt. Close, disconnect,
and daemon loss discard queued work; the original receipt is not retracted.
When communication logging is enabled, later native admission or queue loss is
recorded on the recipient daemon with the original message ID. There is no second
sender receipt. The new Run retains its result and follows the normal completion
notification rule. Peers and already-seeded runs never enter this retry path.
Internal/no-receipt uncertainty is never retried.

#### `turn.status`, `turn.wait`, and `turn.ack`

The Worker SDK owns one ordered cursor, at most 256 run records, with terminal
bodies held only there. The daemon keeps admission/policy metadata and routes
reads; the Caller keeps no run map. `turn.status {session_id,run_id?}` reads a
snapshot; an omitted run ID selects the oldest unconsumed record.
`turn.wait {session_id,run_id?,timeout_ms?}` waits for that same non-consuming
snapshot to become terminal. An explicit timeout may return running; omission
waits without a local polling limit. Both return `{session_id,run_id,state}`:
`running` has no result, `done` includes the native terminal `result`, and
`unavailable` includes a nonempty `reason` without a fabricated native terminal.
A missing or retired record is `unknown_session` while disconnected lanes use
`not_connected`. At most 256 active reads are admitted, independently of the
existing transport correlation bound. Capacity is reserved before new work and
older output is never silently evicted. A full 256-record cursor rejects new
message-triggered work with explicit Busy backpressure; owners must collect and
acknowledge records. Delivery does not silently evict or auto-ack results.

`turn.ack {session_id,run_id}` consumes only the oldest terminal record and
returns `{}`. A repeat acknowledgment of an issued, already consumed ID in the
same generation is idempotent; future/nonexistent IDs are not consumed IDs.
Acknowledging a later retained record is busy. Status, wait, cancellation and
collector replacement do not consume output or reset grace. CLI/stdio callers
acknowledge after successful complete output emission. Pre-cancelled ack submits
nothing; once submitted, the kit settles acknowledgment despite later caller
cancellation. A lost ack response leaves consumption uncertain. Ack means cursor
consumption, not proof a native model or display received the answer.

#### `turn.ready`

The worker sends `{session_id,run_id,state,outcome? ,reason?}` with terminal
metadata only: done includes native outcome, unavailable includes reason.
The daemon releases that admission and, for a native terminal including failed
or interrupted, arms the independent auto-close deadline. `unavailable` without
an observed native terminal releases the slot but arms no new grace. The worker publishes
its cursor on the control acknowledgment, before dispatching a subsequent frame.
No terminal body is copied into the daemon. The control acknowledgment certifies
processing, not native receipt or output collection.

When notification is enabled, the daemon sends an ordinary peer message under
the actual lane identity to its owner or persistent notify target. The body is
only a lane/run collection pointer, never the answer or admission receipt.
The receiving product's normal native delivery policy applies, including mandatory idle wake. No-notify omits it. Failed/unavailable delivery consumes
nothing, extends no deadline and creates no retained notification or retry.

#### `turn.interrupt`

Either a caller sends `turn.interrupt` to the daemon or the daemon forwards it
to the addressed lane. The request carries only `session_id`.

A successful turn.interrupt RPC records or coalesces the interrupt request for the outstanding run. Its empty result does not certify native acceptance or completion. The worker invokes the native primitive at most once for that run. Native callback errors remain the prescribed quoted diagnostic; only the native run terminal supplies the stopping outcome. Idle remains not_running.

There is no interrupt grace timer and no `timed_out` outcome: if the native run
remains unresponsive, the caller closes the session.

#### `session.close`

Either a caller sends `session.close` to the daemon or the daemon sends it to
the addressed lane. The request has optional `forget`, default false. The
worker asks the product to close and always returns `{}`; a product cleanup
error is one quoted line on worker stderr. One constant `closeBound = 10s`, measured from when the daemon sends `session.close` to the Worker, bounds the entire close path. A result before the bound makes the
daemon close the socket, send TERM, and reap; expiry makes it close the socket,
send KILL, and reap with no second waiting period. The spawn/open transaction
bound and `closeBound` remain the two operation bounds; the independent
terminal auto-close deadline is lifecycle policy.

If close arrives during a run, the worker kit interrupts once, awaits that same
run's terminal result, and then closes natively; the whole sequence must fit
inside `closeBound`. If the
bound forces a kill, worker EOF fails the outstanding caller exactly once; the daemon never
fabricates an interrupted result.

After orderly close or unrequested EOF, the durable row is offline and
resumable. A later close of that offline row succeeds without launching a
worker and leaves the row resumable. `forget:true` deletes the offline durable
row directly; for a connected row it deletes only after the worker is stopped.
Neither path opens or deletes product-owned native history. There is no closed
row state. Independent terminal auto-close uses this same Close path; product
adapters own no archival timer.

### 1.2 Edge rules

- A hello with both `session_id` and `launch_token`, with neither, or with fields
  from the other branch is invalid and closes the connection.
- A peer re-hello with the same ID updates name and info only; changed groups
  or group order are `invalid_hello`. A different-ID re-hello ends the old transient identity
  once, detaches its reply sinks, fails pending deliveries once, and installs
  the new peer, private group, and groups before acknowledging on the same
  connection, without a supersession frame. Already-admitted requests retain
  their captured source identity and groups. A worker re-hello is invalid.
- A peer hello whose ID belongs to a lane row is invalid. A worker hello whose
  product differs from its token reservation is invalid; the token lookup is
  the authority for both facts.
- A rowless describe token can authenticate hello but can never authorize
  `session.open`; EOF after describe is the worker's normal exit.
- Worker-originated session methods before the session ID commit are rejected.
  The lane owner commits while handling the successful open response, before
  handling the next inbox frame; commit failure closes the provisional
  connection. The kit adds no additional commit buffer, gate, or
  acknowledgement. After commit, the same connection is the lane's presence
  and uses those ordinary methods; there is no tool frame.
- A second explicit start/run while the native slot is occupied returns busy.
  Message wake uses that same slot, never a second scheduler.
- `lane.spawn` with `resume_session_id` naming a connected row returns
  `already_connected`; lanes never use supersession to manufacture a second
  worker connection.
- New `lane.spawn` requires a name not held by any row on that host; collision
  returns `name_taken`. Every retained row reserves that name until an explicit
  `session.close{forget:true}` deletes the row.
- A peer private group is `session:<id@host>`; a lane private group recursively
  appends `/<leaf>` to its parent's. A new lane receives exactly its parent's
  private group, its own private group, and explicit `extra_groups`; no other
  parent group is inherited. With no extra group, only its parent sees it by
  default. The trusted daemon accepts any explicit extra group.
- Composed lane names and recursive private groups record creation ancestry.
  A later peer or parent rename does not cascade into existing child names or
  group paths, and attached lane titles stay fixed.
- A lane row is claimed for resume, close, or forget until that operation's
  cleanup finishes. New run, interrupt, close, resume, or forget requests for a
  claimed row return `busy`; nothing waits on a row. Delivery to a claimed but
  still attached lane proceeds. Fresh spawn reserves its composed name until
  its product ID is known.
- Resume acquires product-side exclusive ownership before touching the existing
  native session and holds it through cleanup. Fresh open does the same before
  create when the wrapper chooses the ID; when only the product can allocate the
  ID, there is no pre-existing identified resource to fence, so it acquires the
  ID-keyed lock immediately after allocation and before every later mutation.
  Wrappers pass the wrapper host's flock file description into the native
  child, so abrupt wrapper death cannot free the lock while a surviving child
  writes; contention is `spawn_failed` with `session busy`. A native product's
  own mechanism qualifies only when it excludes competing processes.
- Run start/read/wait/ack or `turn.interrupt` addressed to a durable row without
  a connection returns `not_connected`. `session.close` instead performs the
  offline row operation described above. Resume is an explicit `lane.spawn`; a
  caller kit may compose that automatically without changing the wire.
- `turn.interrupt` while no run is outstanding returns not-running. An accepted
  interrupt does not promise that the native product has already stopped.
- Collector timeout/disappearance leaves the Worker cursor intact while the
  worker remains active. Actual lifetime-owner exit separately closes a
  nonpersistent lane and invalidates its cursor. Persistence does not disable
  auto-close or enable wake.
- Worker EOF cancels every callback, never invokes interrupt afterward, calls
  native close once if open committed, fails pending calls exactly once, and
  leaves the durable row offline.
- `session.close` has one 10-second deadline from close admission through process
  reap. Deadline expiry closes the socket, sends KILL, and adds no grace period.
- Same-identity replacement atomically makes the new peer current, sends
  `session.superseded` to the old exact connection, and prevents reconnection by
  that displaced identity instance.
- Delivery is mandatory during active work as well as at idle. Products must
  meet the active-work delivery and BUSY-MID requirements in `message.deliver`.
  A wrapper or daemon queue that waits for the entire active Run to finish is
  not a conforming substitute. Receipt vocabulary is not a capability waiver.
- A worker binary absent from PATH fails as `unknown_product`. A binary that
  starts but exits before hello fails describe or spawn with bounded trailing
  stderr and its exit code when one exists. Neither fabricates readiness.
- Optional `host` on `lane.describe` or new `lane.spawn`, and canonical
  `id@host` identities on later operations, select the same one-hop daemon
  forwarder. An unconnected explicit or canonical host returns `unknown_host`; federation
  never creates a second request shape or retry path.

This protocol deliberately does not compensate for five losses. A daemon crash
before commit can orphan native files; workers exit on EOF and those files are
garbage, not state. A successful spawn reply can be lost with its caller; the
caller kit lists by name before spawning again. A caller can lose a completed
turn result after worker retirement, including explicit/automatic/owner close;
resume does not recover that old answer. Collector loss alone retains it while
the worker lives. No daemon restart or supervisor-replacement output recovery
is implemented by this memory-only cursor. A wrapper can die after truthfully accepting a queued
delivery but before the next turn; that private queue is not durable protocol
state. After a daemon crash, an old worker may take milliseconds to observe EOF
while the restarted daemon spawns a resume; a persisted-PID reap barrier is
refused state, and both workers must obey EOF and native-session exclusivity.

### 1.3 Method convergence

| Today | Universal wire | Reason |
| --- | --- | --- |
| `session.hello` | `session.hello` | Survives and gains the token-discriminated worker branch. |
| `lane.worker.hello` | `session.hello` | Merged; two first-frame methods would duplicate framing and validation. |
| `session.update` | — | Dies; connection presence, the current admission, and `turn.ready` supply live facts. |
| `session.superseded` | `session.superseded` | Survives unchanged so replacement is terminal rather than a reconnect flap. |
| `peers.list` | `session.list` | Merged with lane list and status because peers and lanes are sessions. |
| `message.send` | `message.send` | Survives as the single outbound messaging operation. |
| — | `trace.configure` | Adds live parent-owned tracing without persistent policy or a second delivery transport. |
| `lane.doctor` | `lane.describe` | Renamed because hello success reports support; there is no readiness state. |
| `lane.list` | `session.list` | Merged because a lane is a session row plus an optional connection. |
| `lane.start` | `lane.spawn` + `turn.start` | Creation and the first turn are two ordinary composable operations. |
| `lane.run` | `lane.spawn` + `turn.run` | Synchronous convenience belongs in the caller kit, not the wire. |
| `lane.resume` | `lane.spawn` + `turn.run` | `resume_session_id` selects the durable row; running remains separate. |
| `lane.steer` | `message.send` / `message.deliver` | Steering is mandatory message injection or truthful wrapper queuing. |
| `lane.wait` | `turn.wait` | Non-consuming read of the resident Worker cursor. |
| `lane.status` | `session.list` / `turn.status` | Presence and retained run status remain distinct. |
| `lane.interrupt` | `turn.interrupt` | Merged with the worker-side interrupt under one end-to-end shape. |
| `lane.archive` | `session.close` | Explicit lifetime termination is one session operation. |
| `message.deliver` | `message.deliver` | Survives; its result becomes the truthful closed delivery receipt. |
| `lane.turn.start` | `turn.start` | One admission returning a stable RunRef. |
| `lane.turn.wait` | `turn.wait` + `turn.ack` | Explicit non-consuming read and acknowledged consumption. |
| `lane.turn.interrupt` | `turn.interrupt` | Merged with the caller-side operation. |
| `lane.session.archive` | `session.close` | Merged with the caller-side lifetime operation. |

The closed method authority therefore shrinks from twenty-one methods to eighteen, including the internal worker
execute/ready pair and explicit retained-result acknowledgment.

### 1.4 Federation lifetime controls

Forwarded caller metadata includes origin-minted `owner_lifetime` and the hub's
unique `source_attachment` epoch. The hub validates source identity against the
TLS host and stamps its own epoch; an origin cannot choose that stamp.
`federation.owner_end {host,owner_session_id,owner_lifetime}` is routed to the
registered destination and gains `source` and `source_attachment` downstream.
Hub-only `federation.host_end {host,source_attachment}` ends every lifetime from
that exact departed attachment on every removal path. Their `{}` responses mean
lifecycle event admitted, not native process termination. They carry no output.

Origin forward and owner-end enqueue are serialized. The hub preserves stream
order; destination records ownership before admitting later end events and
rechecks it at publication. Same-ID supersession needs no extra control frame.
Control queue/write failure closes the link instead of dropping cleanup.
Destination hub loss ends all remote nonpersistent owners. Persistent lanes
ignore owner loss and retain their independent auto-close deadlines. This is
connectivity-lifetime policy, not proof of remote process death; no lease,
polling, replay or reconnect subsystem is added.

### 1.5 Error authority

Every correlated failure uses exactly one numeric JSON-RPC code and symbolic
message from this table. Kits match the code, never free-form text.
`spawn_failed` has the closed `data` object
`{exit_code?:integer,stderr_tail:[string]}`; `internal` has a non-empty string
containing the daemon error. No other code has `data`. `exit_code` is absent when process
creation itself failed, because no child existed from which to obtain one. If an
invalid frame has no valid request ID, the daemon cannot correlate a response
and closes the connection without writing one.

| Code | Message | Raised by |
| ---: | --- | --- |
| `-32600` | `invalid_frame` | Any method whose envelope, closed params, or daemon-checked identity grammar is invalid, but only when a valid request ID is recoverable. This includes a composed lane name beyond 128 characters. |
| `-32602` | `invalid_hello` | `session.hello` when its union, protocol, identity, or token is invalid. |
| `-32001` | `unknown_session` | `message.send`, resume `lane.spawn`, run start/read/wait/ack, `turn.interrupt`, or `session.close` when the named row or peer does not exist or is invisible to the caller. |
| `-32002` | `not_connected` | Run start/read/wait/ack or `turn.interrupt` when a durable row has no connection. |
| `-32003` | `busy` | Run admission when the target already has an outstanding run or 256 retained records; out-of-order ack; a cursor read after close admission; a worker loop dequeuing a 257th unanswered call; a full 256-event connection inbox; or a new run, interrupt, close, resume, or forget for a claimed lane row. A delivery rejected by either bound has reason `busy`; delivery to a claimed but attached lane is still admitted. |
| `-32004` | `not_running` | `turn.interrupt` when the target has no outstanding run; or an ordinary worker delivery refused before any native admission at a finished turn boundary, for daemon hold/reseed. |
| `-32005` | `already_connected` | Resume `lane.spawn` when the durable row already has its worker connection. |
| `-32007` | `unknown_product` | `lane.describe` or new `lane.spawn` when the product token is invalid or its binary is absent from the target host's service PATH. |
| `-32008` | `unsupported_open_field` | New or resumed `lane.spawn` when the supplied or stored `open` object contains a field absent from the new worker's hello declaration, or the worker lacks mandatory `supports_message_run:true`. |
| `-32009` | `spawn_failed` | `lane.describe` or `lane.spawn` when exec, hello, open, or native creation fails before commit. |
| `-32010` | `timeout` | `lane.describe` or `lane.spawn` when its one spawn/open transaction bound expires. |
| `-32011` | `not_committed` | Any worker-originated client-to-daemon session method received after hello but before product-session-ID commit. |
| `-32012` | `superseded` | Any request from a peer connection displaced by exact directory replacement. |
| `-32013` | `name_taken` | New `lane.spawn` when another row on that host already holds the requested composed name. |
| `-32014` | `unknown_host` | `lane.describe` or new `lane.spawn` naming an unfederated `host`, or any canonical identity input whose host part is neither local nor connected. |
| `-32015` | `forward_lost` | A one-hop federated request whose transport ends before its response; the request may or may not have been applied on the target host and is never retried. |
| `-32016` | `unsupported_trace` | `trace.configure` or `lane.spawn.trace` when an involved federated host or hub cannot carry or enforce tracing. Upgrade every involved daemon and hub before retrying. |
| `-32603` | `internal` | The daemon's own shutdown or durable row-file operation fails; `data` carries its error text. An explicit uncertain-delivery callback may also use this code; it never certifies native refusal. |

## 2. Daemon

### 2.1 Stored and live data

The daemon is a router around one directory and a set of durable row files. It
does not know how any product creates a session, runs a turn, injects a message,
or closes. Native
products and wrappers expose those operations through the eighteen methods in
Section 1. The daemon contains no product switch, lane actor, product driver,
product capability interface, output ledger, or product archive transaction.
Generic lane policy owns one terminal auto-close deadline and invokes ordinary
Close; it never interprets native state.

Only lanes have durable rows. A row has exactly these columns:

| Column | Meaning |
| --- | --- |
| `session_id` | Immutable canonical `id@host` and primary key; the product returns the ID part from `session.open`. |
| `product` | Immutable binary name executed with empty argv and a launch token in its environment. |
| `name` | Canonical `name@host`; for a lane the stored name part is `<parent name part>/<caller leaf>` and the host is where the lane runs. It is the resume recipe's mirror of the product title, set at open. |
| `groups` | Full resume-membership recipe containing the parent's private group, the recursively composed `<parent private group>/<leaf>`, and explicit `extra_groups`; no other parent membership is inherited. |
| `open` | The original validated `SessionOpenOptions` value, re-marshalled unchanged on resume with `arguments` order preserved. |
| `created_at` | Daemon timestamp assigned when the row commits. |
| `policy` | Normalized independent lifetime, auto-close and notification selection. Older rows without it load as persistent, non-notifying lanes with no auto-close, preserving their original lifetime behavior; their delivery behavior upgrades to mandatory wake. |

Each row is one JSON file named `<sha256(session_id)>.json`. A commit writes and
syncs a temporary file, renames it to that name, and syncs the containing
directory. Startup reads only committed `.json` names; an interrupted temporary
file is ignored. An unknown column, wrong digest name, duplicate session ID, or
duplicate canonical name makes the durable directory invalid.

There are no durable peer rows. The daemon qualifies a peer hello's product-
asserted ID and name parts with its effective host, then creates one transient
directory entry containing that canonical identity, groups, information, and
exact connection. A same-ID re-hello refreshes name and information only; a
different-ID re-hello ends the old identity on that connection and installs a
fresh entry. EOF removes only the exact installed entry. Lane EOF removes only
the live attachment; its row remains offline and resumable.
`SessionSummary.kind` is derived: a durable row is a lane and a transient entry
is a peer.

The in-memory directory indexes canonical IDs, canonical names, and launch
tokens. Each entry contains its row data, current attachment, claimed and
running facts, an identity-lifetime `done` channel, and a parent-lifetime reference.
Local lifetime tokens survive same-ID replacement; remote lifetimes also bind
the origin host and its authenticated hub attachment epoch. One short directory
mutex protects those registry facts together. It is held only for lookup,
insert, replacement, removal, claim, and a copied list snapshot. No socket,
process, disk, channel wait, or callback runs while it is held, and it never
nests with another daemon mutex. An entry object is also the destination token:
a receiver compares that exact object before admitting a request or settling a
response, so a late event cannot attach to a replacement identity.

For a connected lane, live name, groups, attachment, and running status come
from its directory entry, exactly as for a peer. The durable `name`, `groups`,
and `open` columns are the product-title mirror and resume recipe used while
offline.

Every accepted connection has one reader goroutine, one writer goroutine, and
one owner loop. The reader posts frames in order into that connection's
256-element mixed-event inbox. The writer alone writes its bounded outbox. The
owner loop owns request admission, the pending-call map, child and launch
references, timers, and reply handling; it never shares those fields with
another loop. Foreign directory routing posts without blocking, returning
`busy` when the inbox is full. Owned helpers may post back to their own loop
while an owned count keeps that loop alive until every such helper has returned.

Each routed operation carries a capacity-one reply slot created by its sender.
The target loop is the sole code that removes the operation from its pending map
and answers that slot. One sender-owned helper waits for either that answer or
the sender identity's `done` channel and posts exactly one completion to the
sender loop. This leaves one place that settles a target operation and one place
that writes the caller response; it needs no reply lock, result collector, or
durable output.

Daemon start performs no recovery pass. Every durable row starts offline;
connections, pending calls, owner loops, and reservations start empty. Nothing
is replayed or reaped from a prior incarnation because its workers exited on
EOF. Restart is therefore a row-file load, not a lifecycle transition.

Daemon configuration consists of an optional federation hub address, the
daemon's federation secret when a hub is used, an optional host name, and an
optional list of product names matching the product grammar. The `local_key`
setting is specified but has no daemon configuration surface in 0.5.0; the kits
reject every nonempty reserved environment value. With no hub the daemon is
standalone and needs no federation secret; every optional value may be absent.
The effective host is the configured
host name, or the reserved `local` when none is configured. Configuring a hub
requires an explicit non-`local` host name. A hub rejects `local` during the
handshake and closes a later authenticated duplicate at attachment admission,
so `@local` is never federated.
The hub's symmetric configuration is one map from host name to opaque secret,
with a unique secret for every host. The product list is an advertisement for
discovery, not a registry or allowlist: the service PATH alone decides whether
a lane can launch. The daemon validates each configured product name at load,
refuses duplicate names with one error, and treats an empty list as absent.
`sessionbus secret` generates and prints 32 random bytes
encoded as base64; the daemon and hub reject any configured federation secret
shorter than 32 decoded bytes. The future local-key extension has the same
specified minimum, but 0.5.0 does not admit it. Config files containing secrets
must be mode 0600.

No live fact, connection pointer, child process, deadline, caller ID, or
access-check result is stored in a row.

### 2.2 Opening and replacing connections

In 0.5.0 the daemon accepts an owner-protected plain Unix socket, enforces the
framing limits in Section 1, and requires `session.hello` first. The reserved
local-TLS extension specifies reuse of the Section 2.5 derivation and pinning
code with `client` and `daemon` labels, one expected key, and no SNI lookup; it
is not an implemented admission branch. The daemon rejects a peer hello unless
`session_id` and `name` are valid
unqualified name parts, then appends its effective host to both.

For a first peer hello, the directory mutex covers one short replacement: check
the canonical ID against rows and peers, detach any old entry, and install the
new entry. The old entry becomes inadmissible before the mutex is released. Its
owner then writes `session.superseded` as the final frame with
`supersedeWriteBound = 1s` and closes the socket without waiting for an
acknowledgement. No lock is held for that write. A writable socket returns
immediately; the bound only limits a dead peer. The old owner settles its
pending calls once. No reconnect lease or grace timer exists, and a hello from
the displaced entry is rejected like every other request from it.

A second peer hello on that same live connection is handled in frame order. With
the same `session_id`, its declared groups slice must equal the original slice
exactly, including order; the daemon then refreshes name and information in
place. Different groups are `invalid_hello` and close the connection. With a different
`session_id`, the directory replaces the entry before acknowledgement. Each
admitted request already holds a copy of the old source identity and groups;
the old owner detaches its reply sinks and settles pending inbound deliveries
once. The new entry has its own private group and the new hello's groups. It
sends no `session.superseded` on this same socket. This adds one same-ID update branch
and one different-ID replacement branch. A worker connection never accepts a
second hello.

A worker hello resolves a one-use reservation created by `lane.describe` or
`lane.spawn`. The reservation binds token, product, transaction, and expiry.
Validation and token consumption are one directory operation. Wrong-product,
unknown, expired, or repeated hello is `invalid_hello`; cancellation removes an
unclaimed reservation. The connection stays provisional until open commits.
Before commit it may answer daemon calls but every worker-originated session
method returns `not_committed`. The lane owner processes the open response and
commits before it processes the next frame already waiting in its inbox. Commit
failure closes the provisional connection. This commit-before-dispatch ordering
needs no additional commit buffer, gate, or acknowledgement.

Local peers state their identity and groups, and a federated daemon states its
own summaries. The daemon adds no PID, peer-credential, descendant, signature,
or capability check.

### 2.3 Describe and spawn

The product token is exactly a binary name on the daemon's service PATH.
Native and wrapped products have one process contract: exec `<product>` with
empty argv and `SESSIONBUS_LAUNCH_TOKEN` in the environment. Presence of
that variable selects worker mode; absence selects the product's ordinary
entry. Tokens are per-spawn, single-use, and expiring, so an ordinary human
shell never holds one accidentally. The daemon does not supply the reserved
`SESSIONBUS_LOCAL_KEY` in 0.5.0; both kits still consume and scrub the variable
and reject it when nonempty before product code runs.
Callers name the product binary but never supply an executable path.
The daemon has no executable registry, fixed-argument table, or product
allowlist, and no product-specific configuration beyond the advertised names.
Its optional product list only advertises likely availability; an unlisted
executable works, while a listed name missing from PATH is `unknown_product`.
The hub address, effective host name, and host secret configure transport only
and never participate in product resolution; the local key is reserved in
0.5.0.

Wrapped products are our binaries named `claude-peer`, `codex-peer`,
`grok-peer`, `qwen-peer`, `opencode-peer`, `kilo-peer`, `pi-peer`, and
`omp-peer`. Invocation without a launch token is the interactive peer launch;
invocation with one is the resident lane wrapper. Tokens are honest binary
identities: `claude-peer` and `claude` are different products. When Claude
becomes native, `claude` begins honoring the same launch-token environment and
`claude-peer` is deleted; the daemon and protocol do not change. The daemon accepts a product token only when it matches
`^[a-z0-9][a-z0-9-]{0,31}$`; an invalid token is `unknown_product`, so no path
separator reaches process resolution. Failure to find an executable on PATH is
`unknown_product`; an executable that starts and then fails is `spawn_failed`.
The one-use token is present only in the child's environment.

`lane.describe` creates a rowless reservation, starts the process, and waits
for either a valid hello, process exit, shutdown, or the single spawn timeout.
Valid hello is the complete result. The daemon returns its declarations and
stops the supervised process without sending `session.open`. Describe creates no
row, native session, or product-specific readiness state.

New `lane.spawn` validates the full open object and composed name before exec.
It creates one directory reservation containing the composed name, product,
launch token, timer, and reply slot. The composed name is
`<parent name part>/<caller leaf>@<target host>`; the groups are the parent
private group, `<parent private group>/<leaf>`, and explicit `extra_groups`.
The prospective name stays reserved through final process cleanup. Siblings are
thereby unique, and two equal parent name parts with the same leaf collide; the
second spawn receives `name_taken`.

Resume looks up the durable row, checks visibility, and claims that exact entry
before exec. A resume claim has no live attachment until commit. Another run,
interrupt, close, resume, or forget returns `busy` immediately; nothing waits.
The owner reuses the row's product, composed name, groups, and stored open value.
New and resumed launches both start
`<product>` with empty argv and the token in the environment, consume hello,
check declared open-field support, and send one `session.open`. Resume includes
the row's ID part as `resume_session_id`; new open omits it.

The launch helper owns exec and the direct child. Before token claim it selects
among child exit, timeout, shutdown, and claim. After claim it posts process
events to the connection owner and waits for the child. The connection owner
serializes the open response, process exit, timeout, connection close, and
shutdown. A process-exit event records the exit while the reader drains frames
already written before EOF; one later event finishes the request. It records
TERM or KILL intent even when the process-start event is late, then applies that
exact intent when the child arrives. Final cleanup stops the process group and
joins the direct child, performs the final group KILL, then releases the
reservation. A child that exits before hello takes this same cleanup path, so
its descendants do not survive.

For a fresh open response, the owner validates the product-returned session ID,
reserves that canonical ID in the directory, writes the row file, then publishes
the ID, name, and attachment together. An existing peer or row with that ID is
`spawn_failed` with `session id already exists`. Resume requires exact equality
to `resume_session_id`. A disk failure returns `internal`; it removes the
unpublished ID and reservation and does not retry. The owner commits before it
handles the worker's next queued frame, so a request written immediately after
the open response observes the row. There is no additional commit buffer, gate,
or acknowledgement.

If the child exits during open, the reader still posts every frame it read
before its one connection-closed event. The owner records the process exit and
keeps the original spawn deadline while it drains those already-read frames. A
valid open response written before EOF may commit; EOF first fails spawn. No
compensating close is attempted for native files created before commit; that
accepted crash window does not justify another stored state.

Caller EOF never cancels describe, spawn, resume, or close. Only the caller's
reply helper ends; the launch owner continues to commit or final cleanup.
Describe still reaps its probe and a successful spawn still publishes its row.
A lost successful reply is recovered by listing visible lanes by name, not by
replaying spawn.

The bound is the single constant `spawnTransactionTimeout = 60s` for every
product and for both describe and spawn. It is not configuration and has no
per-product override.

### 2.4 Routing, turns, and close

Every request reads the caller from its exact current connection, qualifies
a bare target with the caller's own host, resolves the resulting canonical ID
or name among visible sessions, and then forwards the closed method without
translation. The target name part must satisfy the 1-128-character grammar
before lookup; malformed input is `invalid_frame`, while invisible and absent
targets are both `unknown_session`. The
daemon copies source identity and groups from its current peer entry or lane
entry when the request arrives and keeps that copy with the request;
caller-supplied identity does not cross the route and a later peer identity
replacement cannot alter an admitted call.

Destination-entry validation and the non-blocking post to its inbox are one
directory operation. A full 256-element inbox returns `busy` at that door. The
inbox holds mixed events, not 256 reserved call slots. A request already in that
queue may later receive `busy` when the target owner dequeues it. Multicast
resolution and deduplication happen first on a copied directory snapshot.

The target owner admits requests in inbox order. Its pending map contains at
most 256 unanswered worker calls; a dequeued 257th call returns `busy`. This is
a separate limit from the mixed-event inbox and must not be described as 512
calls. For direct or message-seeded admission, the owner also rejects a second
outstanding run, reserves the contiguous run ID, updates the directory's running
projection, and sends the worker frame before handling a following interrupt. `turn.interrupt`,
`session.close`, and `message.deliver` use the same path. The worker kit owns the
single native interrupt invocation. A `session.close` operation retains its
pending slot through process cleanup.

Each pending operation retains the destination entry token and its capacity-one
reply slot. A worker result removes the operation and answers the slot in the
same owner-loop turn. Before settlement the owner compares the retained entry
with its current identity and directory entry; a response from a replaced
identity becomes `not_connected`, rendered as `no_receipt` for delivery. Worker
EOF settles every remaining operation once. A late response is unmatched and
dropped. Caller cancellation ends only its reply helper; it neither removes the
target's pending operation nor cancels product work. A later result is drained
and discarded at that abandoned reply sink; the Worker cursor independently
retains output until ack or retirement. The daemon stores no product-owned output.

A claimed row rejects every new run, interrupt, close, resume, or forget with
`busy`; it never waits. Delivery is not part of that exclusion: a delivery to a
claimed but still attached lane proceeds through the ordinary inbox and pending
limits. A row removed by a preceding `forget` is `unknown_session`.

On the first admitted `session.close`, the lane owner marks the row claimed and
sends exactly one worker close. The worker kit interrupts and awaits any current
run. A product cleanup error is written as one quoted worker-stderr line and the
worker still responds `{}`; daemon cleanup and the offline resumable row do not
change. A successful close response is held in the caller owner only while that
same target still has a run reply outstanding; a busy close is an admission
failure and is never held. When no same-target run reply remains pending, all
eligible held successful closes are released. The run terminal is written
first, then the close response. No dependency crosses targets.

The one `closeBound = 10s` deadline starts when the daemon sends
`session.close`. Close waits for the worker response, never for delivery of that
response to the caller. On ordinary completion the owner closes the socket and
records TERM. On expiry it closes the socket and records KILL. A process-start
event arriving later receives the recorded signal rather than reconstructing a
different one. Process cleanup kills the group, waits only for the direct child,
and sends one final group KILL. The stderr reader alone owns its read descriptor:
after child wait, an immediate read deadline wakes it, it drains bytes already
buffered without waiting for descendant EOF, closes the descriptor, and keeps a
bounded tail.

After cleanup the row remains offline and resumable. With `forget:true`, the
owner deletes its row file before removing the directory entry; later control or
resume is `unknown_session`. Forced cleanup settles the pending run once and
persists no fabricated result. Cleanup and claim release never wait for writing
the caller response.

For an already-offline durable row, the daemon claims the exact row without
starting a Worker. Plain close releases that claim and returns `{}` with the row
unchanged. `forget:true` deletes the durable row file before removing the exact
directory and name entries. A delete error returns `internal`, preserves the row,
and releases the claim so the caller can retry. Normal visibility checks still
apply; an invisible row is `unknown_session`, and a concurrent claimed cleanup
is `busy`. This row-only path has no product-native history authority.

Once close begins, the kit keeps the existing run slot occupied until process
exit even if the native run has already settled. A later `turn.run` is `busy`
without invoking product code, and delivery during close is rejected with
reason `closing`. This uses the existing run-slot fact; there is no closing
state.

The daemon has three named bounds: `spawnTransactionTimeout = 60s`,
`closeBound = 10s`, and `supersedeWriteBound = 1s`. The first two are lifecycle
clocks. The third bounds one final socket write and never delays a writable
connection. None is configuration or product-specific.

The daemon sends at most one `session.close` request on a worker connection.
A concurrent or later close while that row remains claimed receives `busy` and
the caller retries after cleanup. A second `session.close` frame on the
same worker connection is therefore a protocol violation and closes that
connection without a reply.

Every connection ending closes its file descriptor. The reader then posts the
one connection-closed event. On that event the owner detaches its exact
attachment, settles pending calls, and disposes the outbox. It continues
consuming its inbox until its owned helpers and child are finished and the inbox
is empty. Unrequested lane EOF also kills the process group and reaps the child
before releasing the claim. A resume attempt during that cleanup receives
`busy`. Peer EOF removes only the exact transient entry. A stale EOF cannot
clear a replacement entry.

Daemon shutdown closes every accepted connection, including a token-
authenticated worker that has not committed, prevents new reservations and
commits under the directory mutex, and broadcasts shutdown once. Each owner
disables that select case after receiving it, closes its socket, settles pending
work, and reaps its child. Connection owner loops, readers, writers, and launch
producers are registered before starting; reply helpers are covered transitively
by their owner's owned count. Shutdown returns after the accept loop and all
registered work have ended; nothing commits afterward.

### 2.5 Listing, messaging, and federation

`session.list` copies one coherent metadata snapshot under the directory mutex:
durable rows, transient peers, exact attachments, claims, and running
projections. It performs no disk read and waits on no owner loop. Filtering and
group visibility happen while selecting the snapshot; sorting and result
assembly happen after unlock. No roster cache or projection stream exists.

`message.send` first validates each target's name grammar. It then resolves and
deduplicates the whole visible recipient set before posting any delivery. Every
remaining label yields one receipt:
unresolvable labels are rejected with `unknown_session`, `unknown_host`, or
`ambiguous`, while every resolvable current target receives one
`message.deliver`. Resolution retains the exact destination entry. The target
compares that token when it dequeues the request, and response settlement checks
it again; a same-socket identity change therefore cannot attach an old delivery
or receipt to the new identity. Those deliveries run concurrently, and one response returns
the receipts in label order after session-ID deduplication. There is no delivery
timer. Offline lanes and failed calls produce rejected receipts. A full target
inbox or a target owner already holding 256 unanswered calls produces rejected
reason `busy`. A claimed but still attached lane remains deliverable. If EOF or
supersession ends the target connection before a receipt arrives, the sender
receives rejected reason `no_receipt`; whether product code acted is unknowable
and deliberately not promised. Canonical names are resolved only after
canonical IDs; an input containing `@` is always parsed at the last `@` and is
never retried as a local bare name.

Federation carries the same closed public requests and responses between trusted
daemons but replicates no session state. The hub is a transient request router
whose only roster is the coherent set of authenticated host attachments. It has
no session rows, products, snapshots, updates, disconnect notifications, or
durable state. A daemon learns that roster only by issuing the private
`federation.hosts` request when it admits an aggregate operation.

`session.list` with a canonical `session_id` or explicit `host` is forwarded
directly to that host. An unqualified list captures the current host roster,
runs one host-specific list at each captured daemon plus its local list, and
merges the results at the origin. If a captured host is lost before its reply,
the whole aggregate list fails with `forward_lost`; partial lists have no public
representation. There is deliberately no atomic cross-host list: each
destination takes its ordinary local snapshot when its forwarded call arrives.
With no hub link an unqualified list remains a local list.

For `message.send`, every explicit label is split at its last `@`; the origin
issues one request for each remote label and preserves label order while
deduplicating returned session IDs. A group message captures `federation.hosts`
once and sends one group request to every captured host plus the local daemon.
Each destination resolves names and groups against its own current directory
under the carried visibility groups. Every forwarded request is local-only at
the destination and can never fan out or forward again. A directed canonical
name is never implemented as list-then-forward.

The origin generates one message ID before partitioning a send. Every local and
remote leg uses that same value; remote legs carry it as the private nested
request's `message_id`, and the destination supplies it to the ordinary local
delivery path. The private field is transport accounting, not a second public
message identifier or method.

Hub-to-daemon transport is standard TLS 1.3 from Go's standard library. For a
configured secret and label, key derivation is exactly HKDF-SHA256 with empty
salt, info `sessionbus/v1/<label>`, and 32 output bytes, passed as the seed to
`ed25519.NewKeyFromSeed`. Federation uses labels `host` for the daemon identity
and `hub` for the hub identity toward that host. `sessionbus secret` emits 32
random bytes as standard base64; configured secrets may be longer but are
rejected when shorter than 32 decoded bytes. The complete opaque secret is fed
to HKDF.

The daemon presents a self-signed X.509 certificate over its derived host key
and sends its configured host as SNI. SNI is cleartext by design because the
host name is not secret. The hub reads only its immutable startup map from host
name to secret when selecting the TLS configuration and certificate. Each side
pins only the leaf certificate's Ed25519 public key to its independently
derived expected key. Additional chain certificates are ignored; CA trust,
certificate time, certificate name, and hostname validation are deliberately
not applied.

The key match is the federation identity. An unknown or reserved `local` SNI,
wrong secret, or name/key mismatch fails the TLS handshake before the link is
admitted. The listener transfers every accepted socket to the registry owner
before starting its handshake, so shutdown also closes and joins
unauthenticated sockets. After client-key verification the registry admits the
first attachment for a host; an authenticated duplicate loses and is closed,
leaving the incumbent installed. Configuration is startup-only, so rotation is
an edit followed by restart and reconnect, never mutation of live TLS state.
There is no application authentication frame or separate expiry/revocation
system. A copied mode-0600 config can impersonate that host because the derived
key is the identity.

Visible remote lanes remain controllable without a second public wire.
`session.list` gains optional `host`, mutually exclusive with `session_id`, and
`message.send` gains optional `host` only for its group form. Fresh
`lane.spawn` and `lane.describe` already carry an explicit host; remote resume,
run, interrupt, and close use the last-`@` suffix of their canonical session
ID. Missing or local selectors stay local. A directed unconnected host returns
`unknown_host` before any product, name, or session lookup.

A forwarded request is exactly this private envelope; the validated public
request remains nested without a second public method:

```json
{"jsonrpc":"2.0","id":17,"method":"federation.forward","params":{"from":{"session_id":"caller@alpha","name":"caller@alpha","product":"example-peer","private_group":"session:caller@alpha","groups":["team","session:caller@alpha"]},"request":{"method":"turn.run","params":{"session_id":"target@beta","input":"go"}}}}
```

The complete admitted caller context is captured before a helper starts:
`session_id`, `name`, `product`, `private_group`, and `groups`. A later re-hello
cannot change an admitted call's attribution. `private_group` is
`session:<id@host>` for a peer or the recursively composed creation-ancestry
path for a lane. Fresh remote spawn uses that captured path directly for its
parent and child default groups. Neither the hub nor destination infers it from
arbitrary memberships or looks up the caller after admission.

The private wire has exactly two request methods and their ordinary JSON-RPC
responses; it has no notifications:

```json
{"jsonrpc":"2.0","id":1,"method":"federation.hosts","params":{}}
{"jsonrpc":"2.0","id":1,"result":{"hosts":["beta","gamma"]}}
{"jsonrpc":"2.0","id":2,"method":"federation.forward","params":{"from":{...},"request":{...}}}
```

`federation.hosts` is answered from the registry owner's current authenticated
host map, excluding the caller, with sorted names. It is a point-in-time input
to origin fan-out, not a subscription. `federation.forward` is accepted only
as a request with a positive monotonically increasing link-local ID. An
ID-bearing response must match one pending request and its exact public result
shape, or carry one complete valid public error object. A notification, a
public method on the private wire, a private method on the public Unix socket,
a nested federation method, or a target that is not local to the receiving
daemon closes the offending link.

The admitted mapping is closed:

| Origin operation | Destination and private request |
| --- | --- |
| Directed `session.list` | Explicit `host` or the last-`@` suffix of `session_id`; the destination performs its ordinary local list and visibility filter. |
| Aggregate `session.list` | Capture `federation.hosts`; send a host-specific list to each captured host and merge with the local list. Any lost leg fails the whole call. |
| `lane.describe` | Its explicit non-local `host`; the destination performs the ordinary local describe path. |
| Fresh `lane.spawn` | Its explicit non-local `host`; the destination composes parent/leaf from captured `from.name` and `from.private_group`. |
| Resume `lane.spawn` | The last-`@` suffix of canonical `resume_session_id`. |
| Run start/read/wait/ack, `turn.interrupt`, `session.close` | The last-`@` suffix of canonical `session_id`. |
| Explicit `message.send` | One request per remote input label; the destination resolves the label locally with carried groups and returns its ordinary delivery result. |
| Group `message.send` | Capture `federation.hosts`; one host-specific group request per captured host plus the local leg; the origin merges receipts. |

The hub validates that `from.session_id` and any present `from.name` belong to the
authenticated origin host, validates `from.product` and the complete group
capture, derives exactly one destination from the nested public request, and
routes once. It makes no name, group, row, product-installation, or placement
decision. The destination enters the same local session request dispatcher used
by a Unix-socket caller, substituting the captured identity and a cap-1 reply
slot. Local and forwarded requests therefore share list, delivery, launch,
reservation, routing, and error behavior rather than maintaining a
federation-specific per-method implementation.

One owner goroutine processes each link's received calls in frame order, so
`turn.run`, `turn.interrupt`, and `session.close` cannot reorder during
admission. Helpers only wait for already-admitted results. The origin captures
the exact caller identity lifetime before starting its waiter. An origin loss
ends that reply sink. The independent authenticated owner-end/host-end control
closes nonpersistent children; persistent children continue under their own
auto-close policy. An admitted destination call settles through its worker or
link closure, and a buffered orphan reply may be discarded without consuming
the Worker cursor. A remote
run's terminal response is processed before the corresponding remote close
response is released, matching the local lifecycle.

The destination host-link owner owns its link-local ID space, pending calls,
and cap-1 reply slots. It permits 256 unanswered forwarded calls; admission of
the 257th closes that destination link and settles every pending or accepted
queued call once with `forward_lost`. A reconnect is a new exact attachment and
ID space, so an old reply is never resolved through a host name into it.
Transport loss returns `forward_lost`; no request is retried or replayed. A
configured daemon attachment is one-shot: initial dial failure fails daemon
startup, while a later link loss leaves that daemon local until it is restarted.
Reconnect rows prove a fresh attachment and ID space after restart, not an
automatic retry supervisor. The hub changes only the correlation ID, and the
result value or complete error object is byte-identical on return.

The registry owner alone owns the authenticated host map and processes
accept/authentication retirement, admission, roster requests, route lookup,
and exact-attachment removal. Accept and retirement are disjoint events, so a
failed handshake cannot re-enter authentication. Closing first seals the
accept producer, then closes pre-handshake sockets and admitted links, settles
queued work, and joins link/authentication helpers. Owners exchange copied
events through bounded channels and share no mutex. A false bounded post closes
that exact slow link at the registry call site; it is never interpreted as a
state update because no state is replicated.

Normal attachment retirement is acknowledgment-sealed before its final queue
drain. Whole-router cancellation instead closes every link and abandons reply
sinks whose callers have also ended; it makes no acknowledgment guarantee for
those unobservable terminal replies.

Every encoded private request and response is measured before enqueue under the
same 1 MiB frame bound. If the added forwarding envelope makes an otherwise
valid public request too large, the origin receives `invalid_frame` with data
`forwarded request exceeds the frame limit`, and the request never leaves. No
chunking exists. Replay, durable hub rows, session snapshots, update streams,
distributed locks, automatic placement, multi-hop routes, and capability
negotiation are explicitly outside version 1.

#### 2.5.1 Hub build slice and measured prototype

The hub is green-field. `bus/cmd/sessionbus` adds `sessionbus secret` and daemon
hub configuration; `bus/cmd/sessionbus-hub` reads `-listen` and one mode-0600
strict JSON map from host name to base64 secret, starts the TLS listener, and
joins it on signal. Duplicate JSON keys, duplicate decoded secrets, invalid or
`local` host names, and too-short secrets fail startup. Federation material is
never inherited by product children.

The literal implementation layout is:

| Path | Sole responsibility |
| --- | --- |
| `bus/internal/federation/tls.go` | Stamped HKDF derivation, self-signed certificates, and pinned TLS configurations. |
| `bus/internal/federation/frame.go` | Strict private envelope/result encoding with raw public error preservation. |
| `bus/internal/federation/daemon_link.go` | One daemon-side link owner: ordered send/admission, pending replies, and terminal draining. |
| `bus/internal/federation/hub.go` | Accept lifetime, one registry owner, and one I/O/pending owner per authenticated host. |
| `bus/internal/federation/forward.go` | Closed method table plus source/destination validation. |
| `bus/internal/daemon/federation.go` | Exact hub-link publication, origin fan-out/fan-in, and the synthetic request owner that enters the existing local dispatcher. |
| `bus/sdk/go/protocol` | Two optional public host selectors and retention of the complete decoded error member. |
| `bus/cmd/sessionbus`, `bus/cmd/sessionbus-hub` | Secret/config composition only; no routing owner. |

`bus/internal/conn` is reused unchanged at 94 logical production lines as the
single bounded reader, ordered writer, outbox, and fd-close terminator. The
daemon directory remains the sole local-row lock. Exact attachment identity,
cap-1 reply slots, and joined helpers are retained ownership conventions, not a
new generic router. Making the existing daemon a host-mode engine was rejected:
duplicate-host admission keeps the incumbent rather than superseding it, host
roster requests have no session-row analogue, the 257th forward closes a link
rather than returning busy, and hub-link loss removes no local directory row.
These are the measured unchanged-code reuse candidates; no host/session mode or
callback family is introduced.

The frozen replication prototype is retained only as budget evidence. On exact
base `044de4c`, its sealed corrected patch
`09aaa018c1d2f7b7c25ea70161587118a0bf4e0a4c0868fe425c9042f2a403c7`
measured 704 logical production lines in federation and 1,599 in daemon+conn.
The final signed pull prototype export
`5e71f9b798f7851988a8a94907f034ad2597388ec2a1ae2afc19ca1003d7b365`
measures:

| Production block/file | Replication prototype | Pull prototype | Delta |
| --- | ---: | ---: | ---: |
| `federation/tls.go` | 68 | 75 | +7 |
| `federation/frame.go` | 59 | 60 | +1 |
| `federation/state.go` | 70 | deleted | -70 |
| `federation/daemon_link.go` | 141 | 122 | -19 |
| `federation/hub.go` | 315 | 275 | -40 |
| `federation/forward.go` | 51 | 83 | +32 |
| **Federation total** | **704** | **615** | **-89** |
| `daemon/directory.go` | 316 | 270 | -46 |
| `daemon/federation.go` | 154 | 308 | +154 |
| `daemon/handlers.go` | 281 | 288 | +7 |
| `daemon/helpers.go` | 86 | 74 | -12 |
| `daemon/lane.go` | 147 | 147 | 0 |
| `daemon/launch.go` | 72 | 72 | 0 |
| `daemon/server.go` | 96 | 106 | +10 |
| `daemon/session.go` | 265 | 265 | 0 |
| `daemon/table.go` | 84 | 84 | 0 |
| `conn/conn.go` | 98 | 94 | -4 |
| **Daemon + conn total** | **1,599** | **1,708** | **+109** |
| **Two-block total** | **2,303** | **2,323** | **+20** |

The pre-split pull prototype also changes its then-current
`bus/internal/protocol` from 523 to 526 logical lines for its selectors and raw
error member; the two command surfaces measure
134. The patch delta over exact base `044de4c` is 1,019 logical production lines:
547 federation, 394 daemon+conn, 75 commands, and 3 protocol. The complete
feature allocation is 1,087 because the base already contains the stamped
68-line TLS block. These two numbers are deliberately distinct. The former
245/250 replication table and every cap derived from it are **DEAD**, disproven
first by the measured 295-line block-2 draft and then by the complete pull
prototype. The final hard caps are federation 615, daemon+conn 1,708, commands
134, and protocol 526; a breach reopens design rather than moving accounting.

The required pull proof matrix is deterministic and uses no real product: TLS label/pin
rows; unknown, wrong, `local`, and duplicate host admission; pre-handshake
shutdown; coherent and strictly validated roster capture; both directed address forms; destination
visibility from carried groups; exact result/error; aggregate list loss;
explicit and group message fan-out; captured product/name/private ancestry;
nested remote spawn with origin-sink loss; destination loss and reconnect;
retired-socket separation; queued-call settlement after Closed; in-order
run/interrupt admission; run-terminal-before-close; maximum translated frame;
and 257th pending admission. The runtime gate is two isolated daemons named
`alpha` and `beta`, one hub, and `example-peer` only, with zero product turns.

### 2.6 Package boundary

The retained `github.com/antst/sessionbus` repository contains `bus/`: the
daemon and hub commands, daemon and federation internals, generic connection
and process internals, conformance references, and the public Go and JavaScript
SDKs. Product-named launchers, resident wrappers, plugins, and packaging
projections live in the separate MIT-licensed
`github.com/antst/sessionbus-peers` repository and import only the public Go SDK.
The legacy 0.4.0 tree is not part of either retained architecture.

`bus/internal/daemon` owns the directory, session owner loops, row files,
reservations, local routing, origin fan-out/fan-in, and the identity-substituted
request bridge into that same routing path.
`bus/internal/federation` owns every TLS and cross-host lifecycle.
`bus/cmd/sessionbus` and `bus/cmd/sessionbus-hub` only parse input, compose those
packages, join them on signal, and render results. Product selection remains in
the authoritative target daemon and never enters the hub.

`bus/internal/conn` is the daemon's product-agnostic socket pair. The session
creates the inbox and passes it in. One reader posts frames or one final
connection-closed event; one writer consumes the bounded outbox. Foreign posts
are non-blocking, while the reader and owner-confined helpers use their owned
paths. Closing the file descriptor is the only termination request. The reader
alone closes the connection `done` channel and reports the single termination
event; the owner handles it once and disposes the outbox. Graceful supersession
queues one final frame under its one-second deadline before the writer closes
the descriptor. Hard close, overflow, and shutdown close it immediately. No
product policy enters this package.

`bus/sdk/go/internal/rpc` is the separate full-duplex client used by the public kits.
Its one reader runs admission handlers in frame order; a handler decides state
and returns, while callbacks or waits run outside the reader. One complete-frame
write mutex and one small state mutex protect its client calls and close path.
It is not used to own daemon sessions and does not duplicate the directory or
owner-loop facts.
In 0.5.0 this local RPC transport is plain under the owner-checked filesystem
boundary. The host-to-hub connection uses the implemented, required TLS
transport in Section 2.5; the reserved local-TLS extension adds no RPC branch.

**Amendment 2 (2026-09-08).** `bus/sdk/go/internal/stateroot` owns default
socket discovery. `sessionkit.Socket()` is its only public surface, and
`bus/cmd/sessionbus` calls that surface rather than importing the SDK internal
package. The daemon command owns its durable state-root default because the
root module cannot import an SDK `internal` package. Explicit
`SESSIONBUS_SOCKET` selection remains the first discovery rule from Section 1.

`bus/internal/structuredprocess` retains generic bounded TERM/KILL process
ownership and must not import a product or protocol state type. The public Go
and JavaScript worker and caller kits live only at `bus/sdk/go` and
`bus/sdk/js`; the JavaScript kit is published as `@sessionbus/kit`, and peers
import bus code only through the public SDK paths.

The root Go module is `github.com/antst/sessionbus` at Go 1.24 and requires
`github.com/antst/sessionbus/bus/sdk/go v0.1.0-pre.2`, with the one permitted
relative replace to `./bus/sdk/go`. The independent SDK module is
`github.com/antst/sessionbus/bus/sdk/go` at Go 1.24 and contains no replace.
There is no committed workspace. Root gates run plainly; SDK independence gates
run from its module with `GOWORK=off`. The SDK tag is
`bus/sdk/go/v0.1.0-pre.2` on the same root commit.

Every Go file in `bus/sdk/go`, including generated source, carries
`SPDX-License-Identifier: MIT` and is covered by `bus/sdk/go/LICENSE`. Every
JavaScript SDK source file carries the same MIT identifier and is covered by
`bus/sdk/js/LICENSE`; JSON assets are covered by the nearest SDK license. Every
retained daemon Go file carries `SPDX-License-Identifier: GPL-3.0-only` and is
covered by the repository's root GPLv3 license.

Architecture tests reject every daemon import of the peers repository, every
peer import of `github.com/antst/sessionbus/bus/internal/`, every SDK-module
import outside its own module, and every real product-name token under `bus/`
source except opaque configured values and explanatory documentation. Both SDKs
consume the one authority in `bus/sdk/go/protocol`; the npm package includes
that runtime schema, and the language suites enforce byte identity so no
generated schema or fixture copy can drift silently.

`internal/productruntime` dies completely. Its driver interfaces, per-product
registry, environment carrier, native references, and daemon-facing errors are
the architectural seam this protocol removes. Wrapper-specific native code is
assessed in Sections 3 and 4; it may reuse product primitives, but it cannot
restore a daemon driver interface.

The implementation size contract is measured as final logical production
lines, not as additions hidden behind relocation accounting:

| Surface | Maximum | Constraint |
| --- | ---: | --- |
| `bus/internal/daemon` + `bus/internal/conn` | **1,708** | The retained core is 1,314; the complete origin fan-out/fan-in and identity-substitution bridge adds 394. No federation link owner or TLS state is counted here. The former 1,400 estimate is dead. |
| `bus/internal/federation` | **615** | TLS, strict private frames, one daemon-link owner, one host-link owner per attachment, one host-map registry owner, and target validation. The former 250 estimate is dead. |
| Largest daemon router file | 450 | No product literal, argv parser, or product callback. |
| Durable row files | 120 | Load, write+rename, and delete for the six stored columns. |
| Connection admission | 150 | First hello, both peer re-hello branches, and supersession. |
| Spawn/describe | 200 | Name/token reservation, empty-argv exec, owner-loop decision, 60-second bound, and commit before the next frame. |
| Routing and visibility | 200 | Directory lookup/post, owner-loop admission, reply slots, group visibility, per-label receipts, and last-`@`/bare-name resolution. |
| Close, EOF, and forget | 100 | Claim, settle, optional row delete, and KILL at 10 seconds. |
| Both CLI, composition, and config surfaces | **134** | Daemon/hub composition, secret generation, strict host map, optional hub, host, products, and reserved local-key rejection. The former 100 estimate is dead. |
| Cross-host forwarder | **Included in the 615 federation cap; validator file measures 83** | Host-qualified session ID or explicit host selector; one hop, no state or retry. Origin aggregate fan-out is accounted in the daemon bridge, not hidden here. The former standalone 60 cap is dead. |
| `bus/sdk/go/protocol` | **526** | Existing schema/fixture authority plus the two host selectors and complete decoded error member. |
| `bus/internal/conn` subcap | 150 | One reader, one writer, bounded inbox/outbox, and descriptor close. |
| `bus/sdk/go/internal/rpc` | 200 | Framing, one reader, pending calls, complete-frame writes, and close. |
| `bus/internal/structuredprocess` | 700 | Generic process ownership; current functionality may remain. |
| `bus/cmd/sessionbus` and `bus/cmd/sessionbus-hub` | Included in the 100-line CLI cap | Construction and rendering only; no protocol state. |

The daemon migration must therefore delete at least 8,000 net production and
test lines across the surfaces listed below, before any product wrapper
deletions are credited. A size breach is a design finding, not an invitation to
move the same state machine into a differently named package.

### 2.7 Migration deletion floor

The following files die rather than being retained as compatibility shims.
Counts are physical lines at exact base
`c5b280d8db4fc0069dae50365f3515c6de6ab57e`. Files retained for CLI rendering,
non-native wrappers, state storage, generic framing, process supervision, and
federation are deliberately not claimed here; Sections 3 and 4 decide their
product-side fate.

| Lines | File | Reason | Replacement / rehoming |
| ---: | --- | --- | --- |
| 1,116 | `9f366be:cmd/agent-sessions/codex_host.go` | Product-composed host coordinator and attachment authority die. | Generic daemon construction plus the Codex wrapper in Section 4. |
| 445 | `9f366be:cmd/agent-sessions/codex_host_test.go` | Tests the deleted coordinator. | Daemon transaction tests and Codex wrapper conformance tests. |
| 169 | `9f366be:cmd/agent-sessions/control_retry_test.go` | Tests the deleted side control protocol. | Universal connection pending-call and supersession tests. |
| 41 | `9f366be:cmd/agent-sessions/dsh_lane.go` | Daemon-side DSH driver composition dies. | Universal PATH launch plus the native DSH plugin. |
| 348 | `9f366be:cmd/agent-sessions/federation.go` | Product-aware federation router dies. | The generic one-hop forwarding function. |
| 444 | `9f366be:cmd/agent-sessions/federation_test.go` | Tests the deleted router. | Daemon federation proofs in Section 5. |
| 1,628 | `9f366be:cmd/agent-sessions/lane.go` | Lane actor, parsers, lifecycle, and product dispatch die. | Daemon table/router plus caller-kit composition. |
| 94 | `9f366be:cmd/agent-sessions/lane_names.go` | Actor-derived name authority dies. | Durable-table name index and runtime-map identity. |
| 149 | `9f366be:cmd/agent-sessions/lane_notice.go` | Product-specific notice machinery dies. | Worker cursor read/ack and ordinary lane-identity completion-pointer delivery. |
| 1,245 | `9f366be:cmd/agent-sessions/lane_test.go` | Tests the deleted lane machinery. | Daemon transaction tests and shared kit fixtures. |
| 746 | `9f366be:cmd/agent-sessions/messaging.go` | Product-aware peer/lane routing dies. | Generic daemon resolution and delivery. |
| 662 | `9f366be:cmd/agent-sessions/messaging_test.go` | Tests the deleted messaging router. | Daemon delivery and federation proofs in Section 5. |
| 434 | `9f366be:cmd/agent-sessions/presence.go` | Report/projection presence server dies. | Universal connection admission in `internal/daemon`. |
| 1,257 | `9f366be:cmd/agent-sessions/presence_test.go` | Tests the deleted presence server. | Universal admission, listing, EOF, and swap tests. |
| 68 | `9f366be:cmd/agent-sessions/preparation.go` | Old host preparation composition dies. | Minimal command composition over the universal PATH launch. |
| 43 | `9f366be:cmd/agent-sessions/socket_test.go` | Tests the deleted command-side socket server. | Socket helper moves with retained connector endpoint tests in Section 4. |
| 41 | `internal/daemon/admin.go` | Side-channel admin operation dies. | Ordinary `session.list` and `lane.describe` routes. |
| 164 | `internal/daemon/admin_test.go` | Tests deleted admin routing. | Daemon method-table tests. |
| 22 | `internal/daemon/adapter_authorization_test.go` | Adapter authorization seam dies. | Visibility-as-authority router tests. |
| 53 | `internal/daemon/adapter_claude.go` | Claude attachment adapter dies. | Claude wrapper owns native attachment. |
| 61 | `internal/daemon/adapter_claude_test.go` | Tests the deleted adapter. | Claude wrapper conformance tests. |
| 91 | `internal/daemon/adapter_codex.go` | Codex attachment adapter dies. | Codex wrapper owns app-server attachment. |
| 65 | `internal/daemon/adapter_codex_test.go` | Tests the deleted adapter. | Codex wrapper conformance tests. |
| 53 | `internal/daemon/adapter_grok.go` | Grok attachment adapter dies. | Grok wrapper owns native attachment. |
| 54 | `internal/daemon/adapter_grok_test.go` | Tests the deleted adapter. | Grok wrapper conformance tests. |
| 47 | `internal/daemon/adapter_qwen.go` | Qwen attachment adapter dies. | Qwen wrapper owns ACP attachment. |
| 67 | `internal/daemon/adapter_qwen_test.go` | Tests the deleted adapter. | Qwen wrapper conformance tests. |
| 412 | `internal/daemon/attachment.go` | Attachment transaction engine dies. | One hello admission path and one spawn/open transaction. |
| 140 | `internal/daemon/attachment_test.go` | Tests the deleted engine. | Daemon admission and transaction tests. |
| 314 | `internal/daemon/control.go` | Role-based side control envelope dies. | The universal schema-driven method router. |
| 268 | `internal/daemon/control_test.go` | Tests the deleted envelope. | Shared schema and method-direction tests. |
| 399 | `internal/daemon/control_unix.go` | Side control server dies. | One universal endpoint using `bus/internal/conn`. |
| 189 | `internal/daemon/control_unix_test.go` | Tests the deleted server. | Universal endpoint framing and close tests. |
| 92 | `internal/daemon/lane.go` | Daemon lane transition helper dies. | Direct durable-row transaction functions. |
| 89 | `internal/daemon/lane_test.go` | Tests the deleted helper. | Row transaction table tests. |
| 13 | `internal/daemon/socket_test_helper_test.go` | Helper exists only for the deleted control server. | Helper moves with connector tests in Section 4. |
| 75 | `internal/productruntime/architecture_test.go` | Tests the deleted central driver seam. | Generic no-product-daemon architecture test. |
| 47 | `internal/productruntime/drivers.go` | Product driver interfaces die. | Native kit callbacks and Section 4 wrapper-local primitives. |
| 17 | `internal/productruntime/environment.go` | Hidden daemon-to-product environment carrier dies. | Each wrapper owns its child environment. |
| 23 | `internal/productruntime/environment_test.go` | Tests the deleted carrier. | Wrapper launch tests. |
| 19 | `internal/productruntime/errors.go` | Driver error vocabulary dies. | Closed Section 1.4 wire errors. |
| 29 | `internal/productruntime/fakes_test.go` | Fakes exist only for deleted drivers. | Shared kit fixtures and wrapper-local fakes. |
| 31 | `internal/productruntime/lane_registry.go` | In-process product driver registry dies. | Direct `<product>` PATH resolution with launch-token worker selection in the daemon. |
| 31 | `internal/productruntime/lane_registry_test.go` | Tests the deleted registry. | Product grammar and PATH-resolution tests. |
| 110 | `internal/productruntime/registry.go` | Host dependency/product composition registry dies. | Command composition plus wrapper constructors. |
| 107 | `internal/productruntime/registry_test.go` | Tests the deleted registry. | Command composition and wrapper launch tests. |
| 251 | `internal/productruntime/types.go` | Daemon-facing native structs and capabilities die. | Shared schema types move to their connection, storage, process, or wrapper owners. |

One retained command path also changes ownership and is not counted as a
deletion:

| Retained file | Deleted dependency | Replacement / rehoming |
| --- | --- | --- |
| `9f366be:cmd/agent-sessions/hook.go:38-72` | Its `internal/bridge` hook dispatcher dependency is separated from the Codex lane/App Server primitive. | Hook input and attestation dispatch stay in the command package and use the universal caller/control boundary; only Codex lane primitives move into the resident wrapper. |

This floor is **47 files and 12,263 deleted lines**: 16 command files / 8,889
lines, 20 daemon files / 2,634 lines, and all 11 product-runtime files / 740
lines. It excludes product wrapper and kit migration on purpose, so later
sections may only increase the deletion total, not use this number to hide
replacement code.

## 3. Native product kits

### 3.1 Product contract

A native product links one dependency-free reference kit and supplies six
members. This is the complete product-facing contract:

| Member | Product responsibility |
| --- | --- |
| `hello(cancel)` | Return fixed product, version, supported open fields, ordered extra arguments and explicit `SupportsMessageRun` / `supports_message_run` capability after app-ready. |
| `open(cancel, request)` | Create or resume from the typed request, apply its composed name as the product title where supported, and return the exact product session ID. |
| `run(cancel, run, input)` | Receive exactly one text or delivery seed, start one native turn for the kit-owned `Run`, report a seeded native receipt with `Run.ReportDelivery`, observe terminal, and return it. Go uses `RunInput{Text *string, Delivery *DeliveryRequest}`; JavaScript uses `{text}` or `{delivery}`. |
| `interrupt(cancel, run)` | Ask the native turn identified by that same `Run` token to stop. |
| `deliver(cancel, request)` | Receive the full closed `MessageDeliverRequest` `{message_id,from,body}` and return the truthful closed receipt at the demonstrated native boundary from Section 1 (`written`, `injected`, `queued_for_next_turn`, or `rejected`). |
| `close(cancel)` | Stop accepting work, close native state, and release product resources. |

These are primitives, not a daemon adapter interface. They live in the product
process, receive only closed wire values, and never expose a product type to the
daemon. A product can replace our kit with its own implementation by passing the
same conformance fixtures; no daemon or schema change follows.
`cancel` is a Go context or JavaScript `AbortSignal`; control EOF cancels every
callback, and close cancels remaining work after the terminal boundary.
App-ready means the product can accept `open()` and can serve its first `run()`;
the vendor decides how to establish that fact, and the kit sends hello only
afterward.

The product owns the session ID and title. A successful `open()` returns the ID
that the product itself uses, and resume must return the supplied
`resume_session_id` exactly. The composed `session.open.name` is applied as the
product title wherever a title primitive exists, so the bus and product expose
one name. A lane title is fixed for the lifetime of its worker connection:
native lane mode suppresses automatic retitling, and workers never re-hello. A
product that cannot suppress retitling declares an explicit R5 relaxation in
its ledger: the bus name remains authoritative and the native title may differ.

`permission_mode`, `model`, and `reasoning_effort` are opaque product-native
strings. The kit checks only whether each field was declared; `open()` validates
its value and reports `spawn_failed` with
`stderr_tail:["unsupported value <field>=<value>"]` when unsupported.
`open.arguments` is handed to the product in its exact order. Hello's
`extra_arguments` entries are caller documentation, not a daemon parser. Every
product owns one canonical native-argument builder; an argument selecting the
same native control as a typed open field fails open with `spawn_failed` and
`stderr_tail:["argument conflicts with typed field <name>"]`.

Callback failures map exactly once:

| Callback | Wire result |
| --- | --- |
| `open` | `spawn_failed` with `stderr_tail:[message]`; after the worker exits, the daemon puts its bounded trailing stderr before that tail and, when available, adds the observed nonnegative worker-process exit code if the worker supplied none. |
| `run` | Callback error or invalid/oversized output becomes retained `unavailable`. Only an observed native terminal supplies completed/failed/interrupted. |
| `interrupt` | `{}`; the callback message is one quoted line on worker stderr, and the run terminal remains the stopping truth. |
| `deliver` | An explicit `ProtocolError` with code `-32603` preserves an uncertain-submission RPC failure; the daemon maps it to `rejected/no_receipt`, which makes no non-consumption claim. Other callback errors become rejected receipts with the callback message as `reason` and must denote observed refusal or failure before submission. |
| `close` | `{}` followed by ordinary kit exit; the callback message is one quoted line on worker stderr. |

In 0.5.0 both language kits read `SESSIONBUS_SOCKET`,
`SESSIONBUS_LAUNCH_TOKEN`, and the reserved `SESSIONBUS_LOCAL_KEY`; they consume
and scrub all three values and reject a nonempty local key with `local key
transport not implemented in this build`. The launch token is never logged,
returned, or copied into product configuration. The kit sends the worker
branch of `session.hello` only after `hello()` succeeds. `session.open` is the
only call that invokes `open()`. Until that result is written, the kit has no public
session identity and the daemon rejects its worker-originated session methods.

The kit owns the connection, one native Run slot, and the bounded ordered
run cursor. Generation/contiguous sequence and consumed watermark bind ack
without an unbounded consumed-ID map. The daemon alone owns lifetime policy
and its one terminal deadline. Callers/adapters add no scheduler, result cache,
archive timer or reconnect loop.

The kit creates one `Run` token when it installs the run slot and passes that
same object, with the same per-run cancellation context, to `run()` and
`interrupt()`. `Run.Interrupted()` exposes the kit's coalesced interrupt mark;
the kit sets it before calling `interrupt()`. `Run.Native` is the product's one
product-synchronized slot for its native turn. The product publishes that slot
under its own handoff lock and then rechecks `Run.Interrupted()`; `interrupt()`
reads the slot under the same lock. Whichever side observes the other performs
the one native interrupt. Once `run()` returns, the kit issues no new native
interrupt, including while it writes the terminal result. A product or wrapper
keeps no second starting, active, or interrupt-requested lifecycle bits.
`Run.Done()` closes after `turn.ready` acknowledgment publishes the terminal
cursor and clears the native slot, or on connection retirement when no terminal
can be published. Orderly close settles already-admitted cursor readers after
Done and before product Close; new reads after closing are busy. EOF releases
readers without inventing results. A blocking run awaiting execute acknowledgment
installs its one wait before a deferred close write; the original 10-second close
deadline continues throughout, including admission failure or connection loss.

Before mutating an existing session, a native product must acquire exclusivity
that excludes any competing process and hold it through native cleanup. A
fresh product-minted ID is acquired immediately after allocation and before
subsequent mutation; no lock can truthfully precede the creation of an unknown
key. An in-process duplicate-session check alone is insufficient because it
cannot fence a writer surviving in another process. Products without such a
primitive use the wrapper-host inherited-flock rule in Section 4.1.

### 3.2 Full-duplex lifecycle

One reader validates inbound requests and runs each handler synchronously in
frame order. The handler decides state and returns at once; it dispatches every
product callback and response write outside the reader. This permits open,
deliver, interrupt, or close code to originate an ordinary session method and
receive its response. One writer
mutex preserves complete frames. Independent
request IDs correlate worker-originated session methods and inbound results, so
`deliver`, `interrupt`, `session.close`, and those methods all proceed while
`turn.run` is outstanding.

The `Run` token is installed before `run()` starts. Immediately when `run()`
returns, the kit cancels its per-run context under the slot mutex; from then on
interrupt and ordinary delivery return `not_running` without product code, including during result
mapping and the ready control call. The run handler validates the result
and sends metadata without holding the slot mutex across RPC. Its observed
ready acknowledgment publishes the cursor, clears the slot and closes Done
before the reader dispatches the next frame. A failed control call closes the
connection. A second run receives busy until the original slot clears.
Interrupt marks the slot once and invokes `interrupt()` once; concurrent and
later interrupt requests for the same run return `{}` without a second native
call. After the terminal response and slot clear, interrupt returns
`not_running`. No kit timeout is involved. The run handler alone writes the run
response; close may await the slot's completion signal but never owns that
response.

The worker handles one `session.close`: it first claims an empty run slot or
joins the existing run, invokes the shared interrupt once without awaiting that
callback, awaits Done and settles the already-admitted cursor readers, then
cancels every callback context,
calls `close()` once, writes one response, and closes the socket. A second close
frame is the protocol violation defined in Section 2.4. One `sync.Once`
arbitrates the product's `close()` call
between this orderly path and EOF; there are no close waiters or stored close
results. A hanging interrupt cannot hide an available terminal. This entire
interrupt/terminal/native-close sequence must fit within the daemon's single
`closeBound = 10s`; expiry kills the worker and the kit invents no result. From
the instant close owns the slot,
new runs are `busy`, delivery is rejected as `closing` before product code, and
interrupt returns `{}` without another product call.

The kit owns final process ordering: it calls `close()`, writes the close
response, closes its socket, and then resolves its `closed` signal. The product
awaits that signal before process exit; `closed` is a kit signal, not a seventh
callback. `Worker.Shutdown()` lets the product invoke the same single close path
when its native child dies; it is idempotent, closes the socket, and resolves
`closed` without adding another lifecycle state.

The three stop paths are distinct. `interrupt()` asks the current run to stop;
orderly `session.close` follows the preceding close sequence; control EOF first
cancels every callback context, never invokes `interrupt()` afterward, then
calls `close()` exactly once only if `open()` previously succeeded, and exits.
A describe probe whose connection closes before open therefore exits without a
`close()` call. Worker mode never reconnects with a consumed token.

A peer-mode connection behaves differently only at the connection boundary:
ordinary daemon EOF detaches the dead connection and retries the same asserted
peer identity every fixed `peerReconnectInterval = 2s`, with no backoff,
jitter, or attempt cap. A call made while disconnected fails `not_connected`
and is never replayed. `session.superseded` tombstones that identity instance
and stops retries permanently. A correlated `invalid_hello` response is also
terminal for that identity: the kit surfaces it after `closed` and never turns
the following EOF into a reconnect. The Go peer exposes that terminal value as
`Err()`; a plain `Shutdown()` leaves it nil.
The peer kit keeps one JSON-round-trip snapshot as the desired identity. After
each hello response it compares what it sent with that desired value and sends
the current value immediately until they match; a change crossed with connect
or another re-hello therefore cannot leave the older value installed. Its
`rehello(signal, name, info)` call preserves the product, session ID, and groups. While
disconnected it stores the new desired name and information and returns
`not_connected`.
Its `replace(ctx, identity)` call supplies a complete new identity for a
different-ID re-hello on the same connection. It settles outstanding operations
from the old identity as `not_connected`; reconnect uses the new desired
identity. The desired-identity swap, caller settlement, and removal of the old
wire from new-call admission are one peer-lock operation; calls in the gap fail
`not_connected`. The replacement hello acknowledgement reinstalls that wire
only if its desired-identity generation is still current. `rehello` composes
its name and information from the current desired identity under the same lock,
so a crossed replacement keeps the new session ID and groups.
The product's current title is the peer name. A same-ID re-hello updates that
name and information in place only when the declared groups slice is exactly
equal, including order. A
different-ID re-hello ends the old transient identity and installs the new one
on the same socket; it is not a supersession. Worker mode never reconnects or
re-hellos. The closed types, pending calls, complete-frame writes,
delivery, and worker-originated session-method API are otherwise shared.

### 3.3 Go and JavaScript parity

The public SDK exposes `WorkerCallbacks`, the kit-owned `Run`, and the wire types
generated from the schema:
`HelloDescription`, `ExtraArgument`, `OpenOptions`, `OpenRequest`, `OpenResult`,
`TurnResult`, `DeliverySource`, `DeliveryRequest`, `DeliveryReceipt`, `Identity`,
`SessionSummary`, `HostProducts`, and `ProtocolError`; wrappers and products do
not hand-maintain protocol-shaped duplicates. The worker entry is
`serveWorker(callbacks, env)`, which returns the kit-owned `closed` signal. Peer
mode is `connectPeer(identity, deliver)`, `rehello(signal, name, info)`, and
`replace(ctx, identity)`. A
connection-bound client supplies `list`, `send`, `describe`, `spawn`, `resume`,
`trace`, `run`, `interrupt`, and `close(forget)` and is usable from every callback
without blocking the reader. Go also exports one thin no-hello client:
`Dial(socket)`, `Call(ctx, method, params)`, and `Close`; the caller kit and a
wrapper's private lane socket use that one framed implementation. `Socket()` is
the only public surface over `bus/sdk/go/internal/stateroot`; it returns
`SESSIONBUS_SOCKET` when set and otherwise the documented runtime socket.
`Dial("")`, peer connections, worker connections, and `bus/cmd/sessionbus` use
that value.
Go exports `NewCaller(call)` to place the same typed methods and caller
conveniences over that no-hello client's call function; workers and peers are
the other two uses of the same caller.
The caller kit also owns the tool action vocabulary: exported `Actions` is
`list`, `send`, `spawn`, `describe`, `trace`, `run`, `start`, `wait`, `status`,
`interrupt`, `close`, `forget`, and `ack`, and `Caller.Action(ctx, action, args)`
decodes the wrapper/MCP JSON argument shape and dispatches to the corresponding
typed method or caller convenience. `forget` is `close` with `forget:true`.
An unknown action returns an error for the MCP layer to report as Invalid
params. This dispatcher is at most 40 production logical lines; wrappers do not
carry another action switch.

Go constructs one caller for each worker before `Serve` and binds it to the
worker's current connection through `Worker.Call`; repeated `Worker.Caller()`
calls return that same object, including when called before `Serve`.

`start`, `status`, `wait`, and `ack` are stateless methods over the shared wire.
Go exposes `Start(ctx, TurnRunRequest) (RunRef,error)`,
`Status(ctx, ReadRequest) (RunStatus,error)`,
`WaitContext(ctx, WaitRequest) (RunStatus,error)`, a background `Wait(request)`
convenience, and `Ack(ctx, RunRef) error`. JavaScript exposes the same lower-case
methods as promises with an optional AbortSignal, including `wait(request,signal)`
and `action("wait",request,signal)`. `run` returns RunStatus after a non-consuming
wait. `ack` rejects pre-cancelled calls and settles after submission without
using later caller cancellation. No Caller-local ID, result map, target map,
timer, or reconnect loop implements these operations.

Both languages share native-result validation, cancellation drain, ordered
cursor, capacity, reader-close and EOF behavior. Peer attachment reconnect
remains the existing 2-second transport rule; workers do not reconnect. Explicit
user wait timeout is a read limit only; daemon policy owns auto-close.

The Go host and JavaScript worker mode implement the preceding algorithm, not
two interpretations of it. Each uses a small interpreter over the published
schema with no external validation library. Both are tested against
`bus/sdk/go/protocol/session.fixtures.json`; both caller kits also execute
`bus/sdk/go/protocol/caller-sugar.fixtures.json`. Both run the same ordinary
table-driven lifecycle cases with fake product callbacks and a fake duplex
connection. The schema and fixtures are published for vendors to test their
kits. The lifecycle cases are:

1. app-ready hello, one open, and worker-originated session methods rejected before the open
   result but accepted on the same connection after it;
2. describe hello followed by EOF before its acknowledgement, proving open and
   close are never called;
3. completed, interrupted, and failed run results, including empty output and
   oversized-output unavailability and preserved product-reported truncation;
4. a blocked run plus a second run rejected before product code;
5. concurrent interrupt requests for one run, with the kit mark visible on the
   shared token, exactly one native interrupt callback, and no native turn
   created when interruption wins the product handoff;
6. delivery and a worker-originated session method completing while run remains blocked;
7. close during run, with the terminal run response written before the close
   response;
8. control EOF during run, with all pending calls failed and product close
   invoked once;
9. peer EOF reconnect within one fixed interval with the same identity, a call
   while disconnected failing without replay, supersession stopping retries,
   same-ID re-hello updating name/info with identical ordered groups, changed-
   group rejection, and different-ID identity replacement on the same socket;
10. a daemon-to-worker `session.hello` rejected as a wrong-direction request
    before a product callback; worker-originated re-hello rejection belongs to
    daemon admission;
11. `run()` returning, terminal processing paused, and then interrupt, proving
    `not_running` and no native interrupt call before the terminal write;
12. delivery first and another delivery during close, proving cancellation of
    the first callback and `closing` before a second product call;
13. a non-run callback that originates a session method and receives its response;
    and
14. endpoint selection plus single launch-token/reserved-local-key reads and
    environment removal, with a nonempty local key rejected before connect and
    an empty value followed by connect failure and process exit without reconnect;
15. `Run.Done()` remaining open until the terminal response is written and the
    run slot is cleared; and
16. product-requested `Worker.Shutdown()` resolving `closed`, with a second call
    doing nothing; and
17. a failed terminal write entering the connection close path, closing
    `Run.Done()`, and emitting no terminal frame.

There are no product names, product IDs, clocks, sleeps, or network sockets in
the fixture data. Tests control every callback and frame boundary
deterministically.

The size contract is final logical lines:

| Reference surface | Production | Tests |
| --- | ---: | ---: |
| Go transport (`bus/sdk/go/internal/rpc`) | 200 | 250 |
| Go worker host | 205 | 250 |
| JavaScript client plus worker mode | 200 | 250 |
| Go caller kit | 250 | 400 |
| Go no-hello client (`client.go`) | 30 | — |
| JavaScript caller kit | 150 | 150 |
| Reference caller | 200 | 200 |
| Reference worker | 250 | 150 |
| Shared wrapper host | 400 | 400 |
| Shared lifecycle fixture data | — | 220 |

Generic connection framing is counted in Section 2, not duplicated into either
worker host. The no-hello client's tests are counted in the Go caller-kit test
budget; it has no separate test allowance. A kit
exceeding these limits has grown product policy or a third lifecycle fact and
must be simplified.

### 3.4 DSH: the first native worker

DSH's product token and binary name are `dashi`. DSH uses one Sessionbus
plugin and one connection per DSH root session. In
ordinary product mode the plugin sends peer hello without a launch token and
reconnects after daemon EOF. In lane mode it captures and scrubs the token,
waits for DSH app-ready, and sends worker hello on that same socket. It does not
also publish peer presence: successful `session.open` turns the worker
connection itself into the lane's presence, tool path, and delivery path.
Its read-only `/sessionbus` command reports the current connection mode,
canonical identity, and attachment facts without mutating either product or
daemon state.

The DSH `sessionbus` profile is exactly headless DSH core plus the unified
plugin configured `mode: lane`; it disables DSH automatic title changes after
open, and no TUI, second comms plugin, lane extension, relay, or local socket is
loaded. Its package profile is
`{"bundles":["@deepseek-ai/dsh-base","@deepseek-ai/dsh-headless"],"patchReload":"startup"}`
under `dsh.profile`, and its `cordis.patch.yml` inserts exactly
the `session-controller`, `workspace`, and
`{id: sessionbus, name: '@sessionbus/dsh', config: {mode: lane}}` rows,
plus the `workspace-write-noninteractive` permission-preset override, after
bundle patches and before any `--patch` overlays. A non-disabled plugin row is
load-mandatory by DSH boot semantics: import failure exits one with "plugin tree
failed to load". Phase 2 makes the `dashi` boot layer select this exact profile
and `mode: lane` plugin configuration whenever
`SESSIONBUS_LAUNCH_TOKEN` is present; no command-line switch or alias is
involved. Without that variable, the normal DSH profile runs the same plugin in
peer mode. Forcing the plugin's lane mode without a launch token is a startup
error rather than an accidental peer.

The c5b280d integration already proves the DSH core primitives the unified
plugin needs:

- `appReady.onReady` and `appExit` gate hello and terminate headless mode;
- `sessionController.create` and `resolveAgent` create or resume an exact
  session; `rename` applies the composed product/bus title, and `selectModel`
  applies a `provider/model` value but currently also changes the deployment
  default (the upstream ask is a session-only selector);
- `permissionPresets.names` and `permissionPresets.set(session, name)`, plus
  `sessions.flush`, commit the open configuration; `apply` is private and is
  never part of the integration;
- `createUserMessage` with the exact `agent.followup(message)` call starts the
  one requested turn. At c5b280d this is the effective call made by
  `integrations/dsh/lane/plugin.cjs:118` as `agent[mode](message)`, with
  `internal/products/dsh/lane.go:157` supplying the constant mode `followup`;
- root-context `ctx.on('session/event', (session, event) => ...)`, together with
  `session/created` and `session/disposed`, supplies input receipt, turn start,
  assistant text, and terminal reason; no polling is required;
- `agent.cancel` interrupts, and `agent.whenIdle` supports orderly close;
- `agent.steer` admits an interactive message during a run or starts work
  while idle. A durable `session.append` alone is insufficient because it
  does not wake the agent. The plugin must prove the native admission receipt
  and preserve delivery across the final-step boundary; and
- `tools.register` exposes the product's Sessionbus tool while the kit's
  client-to-daemon session-method API carries its calls on the same socket.

Fresh open creates the DSH session only after the typed request arrives,
applies the composed name, and returns its exact ID. Resume resolves
`resume_session_id` and returns that same ID. DSH declares
`extra_arguments:[]`; a nonempty `open.arguments` fails with `spawn_failed`.
Run submits one user message, converts the observed DSH terminal reason to the
three wire outcomes, and carries the DSH reason kind as
`native_stop_reason`. An idle lane delivery is a full delivery seed passed to
`run`; the plugin advertises `supports_message_run:true`, renders the original
source and body once, reports native admission through `Run.ReportDelivery`,
and returns the observed native terminal. Interactive delivery uses the
native wake-capable path. No idle delivery may remain as passive context. In peer mode, a DSH rename
sends a same-ID re-hello with the new title and unchanged groups, updating the
bus name in place. Lane open applies the daemon-composed title and a lane never
re-hellos. The registered Sessionbus tool exposes the
caller kit's start/wait/status/interrupt/spawn/describe/close/list/send surface
defined once in Sections 4 and 5. The close callback cancels if needed, waits
on the currently unbounded `agent.whenIdle`, flushes the session, and returns;
`closeBound` therefore exposes a deliberate supervisor-KILL path until DSH
offers a bounded primitive. A separate outer plugin task awaits the kit's
`closed` signal and then calls `appExit(0)`. After open, control EOF follows the
same product cleanup and exit path; a describe EOF has no native close.

The DSH-specific layer is capped at 300 production and 300 test logical lines,
excluding the generic JavaScript kit and shared fixtures. Its conformance result
must state: **contract learned nothing from the DSH adapter: yes**.

### 3.5 DSH migration

The entire Go package `internal/products/dsh` dies: 10 files and 1,031 physical
lines at c5b280d. Product probing moves to `lane.describe`; lane process
ownership moves to the generic daemon supervisor; permission, model, session,
turn, and delivery translation move inside the native plugin where the DSH
primitives exist.

The separate `integrations/dsh/lane` package also dies in full: 5 files and 464
lines. Its extension registration, presence-served lane RPC, environment
translation, second package manifest, and second Cordis patch are all forbidden
by the one-plugin/one-connection design. The nine-line
`integrations/dsh/comms/prepack.cjs` dies in favor of the repository's generic
packaging step.

`integrations/dsh/comms` remains as the source location but its package identity
becomes `@sessionbus/dsh`; `dsh-comms` is a deprecated compatibility alias
only during packaging migration. Its plugin and tests are rewritten
around the shared JavaScript kit; its Cordis patch installs peer mode, while the
headless `sessionbus` profile pins lane mode. Package/install inventory must
contain one DSH integration artifact, not the old comms-plus-lane pair.

DSH also applies `$DSH_HOME/cordis.patch.yml` (default
`~/.dsh/cordis.patch.yml`) to every profile above per-profile patches and below
`--patch` overlays. The installer writes one `@sessionbus/dsh` insert there
and places the package where every profile's module walk resolves it, for
example `$DSH_HOME/profiles/node_modules/@sessionbus/dsh`. DSH currently
heals `profiles/node_modules` only for its own dependency closure; teaching it
to preserve this external package is the upstream packaging ask. The `dashi`
launcher retains its exact DSH version pin and, for peer launches, maps its
`-g` values to the JSON `SESSIONBUS_GROUPS` environment.

The DSH migration therefore adds **16 more deleted files and 1,504 deleted
lines** before rewriting the retained unified plugin. Combined with Section 2,
the signed deletion floor becomes **63 files and 13,767 lines**. The DSH tree
must remain net-negative after the kit is accounted separately.

## 4. Non-native resident wrappers

### 4.1 Shared wrapper boundary

| Ledger item | Decision | Reason |
| --- | --- | --- |
| Process and connection | Lane mode starts one resident wrapper, which owns the product session, worker connection, one caller-kit instance, and one private Unix endpoint. In peer mode the resident owner is the process the product keeps for that session: a launcher-owned wrapper, a product-kept helper, or a product-daemon-spawned per-session helper, as named in the product ledger. | The shared Worker cursor retains output independently of the collector; adapters keep no caller result cache. A launcher-owned interactive child keeps its inherited terminal while the wrapper propagates signals and exit status. |
| Installed entry forms | One installed integration image is named `<native>-peer`. With a launch token it holds the worker connection and owns a headless child. Without a token its product ledger identifies the resident peer owner; a short-lived `<native>-peer mcp` hops to that owner, while a product-kept helper may be the owner itself. | The environment is the mode discriminator for lane workers; peer ownership follows the product's observed process lifetime, and no product-specific worker flag enters the daemon. |
| Peer identity and name | Identity is fixed before peer hello from the exact environment table below when present; otherwise Claude resolves its parent through `claude agents --json` (`9f366be:cmd/agent-sessions/connector.go:449-487`), while DSH/OpenCode/Kilo/Pi/OMP read their in-process session ID; Codex defers hello until the first tool call supplies `_meta.threadId` (`9f366be:cmd/agent-sessions/connector.go:247-251`). Fresh Grok and Qwen peer launchers mint one product-compatible UUID and pass it both through native `--session-id` and `SESSIONBUS_SESSION_ID`; a hand-started Grok or Qwen process without either source serves every Sessionbus tool with an error naming the required launcher and never sends hello. The product's current title is the hello name; a supported retitle sends a same-ID, same-groups re-hello. | Identity and title are launcher or product facts, never guessed process-global values. Re-hello mirrors one product name on the bus without adding an update method. |
| Product boundary | The wrapper exposes the six Section 3 callbacks locally and contains every product import, argument translation, native protocol, and delivery compromise. | Deleting one wrapper when a vendor adopts the native kit must require no daemon, schema, or caller-kit change. |
| Child launch and title | The lane wrapper connects and sends worker hello before starting a native child. It receives and validates `session.open`, then spawns the child with the stored cwd, model, reasoning, permission, ordered argument values, and composed name as the product title wherever the product supports one. It never observes or publishes later native retitles. If a product cannot keep that title fixed, its ledger names the R5 relaxation and the bus name remains authoritative. | Process-level flags and titles are ordinary open fields for wrappers because the product does not exist until open. Native products start before open and therefore need session-level primitives instead. A fixed lane identity cannot silently follow product auto-title churn. |
| Child lifetime | If the native child dies while idle, the wrapper reaps it and exits immediately; worker EOF makes the row offline and explicitly resumable. | A live worker connection must never advertise a dead product or synthesize an internal restart policy. |
| Native-session exclusivity | Before touching a resumed session, or before fresh creation when the wrapper chooses its ID, the wrapper opens `dirname(SESSIONBUS_SOCKET)/locks/<product>/<session_id>` with `O_CREAT` and takes an exclusive flock. It never treats file existence as ownership: a stale file is harmless and only a live flock blocks. When only the product can allocate a fresh ID, the wrapper locks immediately after allocation and before every later mutation. It passes that same open file description to the native child as an inherited descriptor and holds it through cleanup. The OS releases the lock only after every holder exits; contention fails open as `spawn_failed` with `session busy`. A native product may replace this only with its own cross-process exclusion of competing writers. | An ordinary wrapper death must not release ownership while its child can still write. The inherited flock provides a death-safe process boundary without PID state, a reap registry, or daemon product knowledge; locking an unknown not-yet-minted ID would be fictitious. |
| Tool ingress | In lane mode the resident wrapper owns a private Unix socket under `dirname(SESSIONBUS_SOCKET)/lanes/`, unlinks it on exit, and passes that path as `SESSIONBUS_LANE_SOCKET` to the product-spawned `<product>-peer mcp` helper. The helper sends one `{action, arguments}` value to that endpoint and returns its one result. In peer mode the helper uses the same hop only when the product keeps another session owner resident; otherwise the helper owns the peer connection and caller itself. | Each session has exactly one daemon connection and one caller-kit instance. A per-session Unix path cannot be reused by a stray child as a different session's endpoint, and a short-lived helper never becomes presence or owns caller state. |
| Shared MCP entry | `wrappers/mcp` is one stdio MCP server for the caller-kit tool surface. Its private backend carries one action request to a resident wrapper; its direct backend uses the caller owned by a product-kept helper. Each product adds only `<product>-peer mcp` dispatch and the backend selected by its observed lifetime. A raw framed method hop remains test-only. | One MCP implementation prevents every wrapper from rebuilding tool JSON, while the backend keeps `start`/`status`/`wait` in the actual resident process. Budget: **200 production / 200 test logical lines**. |
| Local encryption handoff | **Reserved in 0.5.0, not implemented:** thin peer launchers and lane wrappers must not configure local TLS. Both kits consume and scrub `SESSIONBUS_LOCAL_KEY` and reject a nonempty value before opening the daemon connection; private MCP/plugin endpoints and native children never receive it. | The implemented local boundary is the owner-checked `0700` runtime directory and `0600` Unix socket. Host-to-hub TLS remains implemented and required; private wrapper hops are not daemon connections. |
| Delivery and run handoff | Idle lane delivery enters one shared Worker seeded Run. Active native admission and completion must be synchronized so a delivery cannot remain in a passive FIFO after the turn ends. A product may defer input only when it will process it automatically. Never replay after uncertain native submission. | Message receipt is separate from terminal completion; a sent message must cause work without another prompt. |
| Interactive delivery | An interactive integration must wake its native agent on idle delivery and admit active delivery through native scheduling. It owns native turn creation; the daemon does not issue a lane Run to a Peer. | Interactive and lane products share the same communication requirement, using their respective native lifecycles. |
| Caller tool surface | Every product exposes the same caller-kit start/wait/status/interrupt/spawn/describe/trace/close/list/send operations; product plugins do not invent wire methods. | Tool presentation is kit sugar over the eighteen methods and is identical for native and wrapped products. |
| Shared size cap | Wrapper host, private MCP/plugin endpoint, and delivery handoff together: **400 production / 400 test logical lines**. | Product-independent scaffolding larger than the daemon router would be a second protocol implementation. |
| Shared deletion | Delete the 16 shared files in `internal/launcher`, including `lane_grok_test.go` exactly once (2,199 lines), and `9f366be:cmd/agent-sessions/connector_refresh.go` plus its test (333 lines). Rewrite `connector.go`/test as the resident peer wrapper's connection, and rewrite `native_peer.go` as thin exec-time product configuration; the stdio entry is only the private action helper. | The old launcher package still dies: CLI parsing and thin peer plans move to `cmd`, product and connection ownership to wrappers, and lane recipes to wrappers. Connector self-exec/release refresh is unnecessary when the resident wrapper holds the connection. |

The launcher/daemon environment contract is exact:

| Variable | Value | Producer |
| --- | --- | --- |
| `SESSIONBUS_SESSION_ID` | Bare product session ID part. | Peer launcher only. |
| `SESSIONBUS_SESSION_NAME` | Bare, unqualified name part. | Peer launcher only. |
| `SESSIONBUS_GROUPS` | JSON array string containing the asserted groups. | Peer launcher only. |
| `SESSIONBUS_SOCKET` | Named daemon Unix-socket endpoint. | Peer launcher and daemon lane spawn. |
| `SESSIONBUS_LOCAL_KEY` | Reserved local-TLS secret; nonempty values are rejected in 0.5.0. | Kits consume and scrub it; no 0.5.0 launcher or daemon configures it. |
| `SESSIONBUS_LAUNCH_TOKEN` | One-use expiring worker reservation token. | Daemon lane spawn only. |
| `SESSIONBUS_LANE_SOCKET` | Wrapper's per-session Unix socket for the stdio MCP helper. | Lane wrapper only. |

Peer launchers pass ID, name, groups, and socket through the product to its
MCP/plugin child. Lane launches contain only socket and launch token; canonical
lane identity arrives later in `session.open`. If the reserved local-key value
is present, the kit scrubs and rejects it instead of passing it onward.

### 4.2 Claude Code

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | `claude-peer` with a launch token owns one long-lived `claude -p --input-format stream-json --output-format stream-json --verbose --replay-user-messages` child. Without a token, `claude-peer` launches interactive Claude; Claude's product-spawned Sessionbus stdio MCP server owns the peer connection. | The lane stream is proven at c5b280d `internal/products/claude/lane.go:112-145`. Peer presence needs no TUI wrapper and continues to work for hand-started Claude sessions. |
| Open and resume | Fresh mints a product-compatible UUID, passes its bare part as `--session-id`, applies the composed title with `--name`, and returns that ID; resume uses `--resume <resume_session_id>` and reapplies the stored open object. Claude supports all five open fields: `cwd`, `permission_mode`, `model`, `reasoning_effort`, and `arguments`; model maps to `--model`, effort to `--effort`, and the protocol's default permission maps deliberately to `--permission-mode dontAsk`. | c5b280d maps the existing stream flags at `lane.go:112-141`; Claude's product CLI help exposes `--session-id`, `--name`, `--model`, `--effort`, and permission mode. The wrapper, not the daemon, owns ID minting and the single interpretive permission mapping. |
| Readiness and projection | The wrapper hellos immediately after Claude starts because it minted the session ID. Claude's `system/init` is verified when it arrives with the first turn, and its `session_id` must equal the minted one. An exit before the first turn is reported by the ordinary process-exit path. Interactive projection lands with the `claude-peer mcp` entry, not the lane wrapper. | No readiness timer. |
| Run | Write one stream-json user frame, keep the run callback pending, and convert the exact result frame to the terminal result. | `lane.go:173-225` already proves the single stream write plus terminal observation. |
| Tools | In lane mode `claude/.mcp.json` starts stateless `claude-peer mcp` calls against the wrapper's private Unix endpoint. In peer mode Claude keeps one stdio helper alive for the session; that helper owns the direct peer connection and its caller. After Claude `/clear`, the helper observes the new product session and performs a different-ID re-hello before serving its actions. | The two resident owners never coexist for one session; `/clear` ends the old transient peer identity instead of creating an `inactive` side state. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt writes the native `control_request` subtype `interrupt`; close ends the stream and reaps the exact child. | `lane.go:277-321` proves both operations and their native acknowledgements. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open fields: **none**. Passive lane-delivery FIFOs are not permitted. | Every open value has a native process flag or stream mapping; native handoff remains inside the product and the daemon contains no product branch. |
| Size cap | **360 production / 400 test logical lines**, including stream framing and Claude argument translation but excluding the shared wrapper host. | The current 578-line actor combines generic lifecycle with product translation; the generic kit removes that duplication. |
| Deletion inventory | Delete all `internal/products/claude` (3 files / 1,108 lines), `internal/launcher/{claude_peer.go,claude_peer_test.go}` (2 / 183), `internal/bridge/claude_title*.go` (2 / 99), and the orphan `internal/bridge/claude_sdk_socket_*.go` family (4 / 178). Rewrite `claude/.mcp.json`; retain product docs/skills. Total: **11 files / 1,568 lines**. | The wrapper owns title and stream/socket integration directly; the daemon driver, bridge title observer, orphan SDK socket split, and old launcher disappear. No compatibility adapter remains. |

### 4.3 Codex

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | `codex-peer` with a launch token owns one session-specific App Server client/subscription. Without a token, `codex-peer` launches interactive Codex; Codex's product-spawned Sessionbus stdio MCP server owns the peer connection. | The lane surface is already App Server RPC (`internal/bridge/codex_native.go`). Peer presence does not require the lane wrapper or a host-global coordinator. |
| Open and resume | Fresh performs `thread/start`, returns the product thread ID, applies the composed name through `thread/name/set`, and materializes its rollout; resume uses `resume_session_id` and reapplies the stored open object. It supports all five open fields. Permission mapping is exact: default means approval `never` with the configured sandbox, while bypass means approval `never` plus `danger-full-access`. | `CodexStartRequest` and `CodexLaneTurnRequest` at `codex_native.go:51-70` expose cwd, model, reasoning effort, permissions, and arguments. The wrapper preserves the product-owned thread ID and name instead of inventing daemon aliases. |
| Run | Send one `turn/start`, await the matching `turn/completed`, and extract the final agent message. | `internal/products/codex/lane.go:75-122` and `codex_native.go:528-656` prove the end-to-end primitive. |
| Tools | In lane mode the wrapper supplies a private `codex-peer mcp` Unix endpoint in App Server `thread/start` `mcp_servers`; calls are stateless action hops. In peer mode the App Server daemon starts one helper per thread with no launcher environment; that helper owns the thread's peer connection and caller and defers hello until the first thread identity exists. After Codex `/clear`, the new thread gets its own helper and peer identity. | MCP configuration and peer ownership are per thread; the App Server's observed helper lifetime removes the old transient identity without `inactive` or a launcher-owned endpoint. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt calls `turn/interrupt` for the exact active turn. Close archives the thread and unsubscribes before the wrapper exits. | `internal/products/codex/lane.go:124-158` and `codex_native.go:758-780` are the existing native boundaries. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open fields: **none**. Passive lane-delivery FIFOs are not permitted. | App Server exposes every typed open value; idle lane delivery uses the ordinary native turn start through the shared Worker. |
| Size cap | **700 production / 700 test logical lines**, including App Server framing and session code but excluding the shared wrapper host. | Native protocol code must be counted with the product that requires it; host-global coordination is forbidden. |
| Deletion inventory | Delete all `internal/products/codex` (2 files / 331 lines) and `internal/launcher/{codex_peer.go,codex_peer_test.go}` (2 / 786). Total: **4 files / 1,117 lines**. | The App Server lane primitive is rehomed into wrapper-owned code; the daemon driver dies and the peer exec plan moves into thin `cmd` composition. |

### 4.4 Grok

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | With a launch token, `grok-peer` owns one private leader, one authenticated ACP primary, and one observer for the exact lane session. Without a token, the launcher owns one private leader, one forever-quiet authenticated startup hold, and the inherited-terminal TUI child; it propagates signals and the TUI's exit status and joins all three. The private leader starts one resident `grok-peer mcp` helper for each native session. Each helper owns that session's one peer connection and caller, using the immutable product-injected `GROK_SESSION_ID`; `/new` starts another helper and peer while the product keeps the former session resident. Headless-only flags are rejected with an error naming lane mode. | Grok Build 1.0.13 keeps each configured stdio MCP helper alive for its session and injects `GROK_SESSION_ID` and `GROK_LEADER_SOCKET`; the launcher supplies the inherited Sessionbus socket and groups. The helper, not the launcher, is therefore the product-observed resident owner. Its private leader still needs the quiet hold before the TUI starts. A headless invocation auto-started a detached default leader which outlived the command, and that stale leader held later interactive startup at `Starting session`; the launcher-owned private leader makes process ownership explicit. |
| Open and resume | The resident wrapper receives `session.open` before it starts the private leader. For a fresh lane it holds `locks/grok/<launch-token-digest>`, uses the same digest for the private lane-socket path, starts Grok without `--session-id`, and calls ACP `session/new`; Grok's returned ID becomes the session ID, then the wrapper renames the lock to `locks/grok/<session_id>` without replacing an existing lock and applies the composed title through the observer rename primitive. Resume locks `locks/grok/<resume_session_id>` directly, starts no argv resume selector, and calls `session/load` for that ID. It puts `--permission-mode`, `--reasoning-effort`, `-m`, and ordered `arguments` on the process command line, with `cwd` as the child working directory. | Grok Build 1.0.13 ignores a fresh `--session-id` in this ACP entry; `session/new` returns the product-owned ID and `session/load` is the sole resume selector. ACP `_meta` exposes only `yoloMode` / `autoMode`; it is not an open-field transport. All five fields and the title are applied at open. The existing 15-second startup hold remains inside `spawnTransactionTimeout = 60s`. |
| Run | Call ACP `session/prompt`, consume only update notifications carrying that prompt's ID, and return its stop reason and accumulated output. Notifications from any other product turn are ignored. | One Sessionbus run owns one native prompt; an unrelated product turn cannot become its result. `grok_native_session.go:270-308` is the resident prompt primitive. |
| Tools | In lane mode the wrapper publishes `lanes/<launch-token-digest>.sock`, and each stdio helper is a stateless action hop to the lane-owned caller. In peer mode the product-spawned resident helper serves MCP in-process over its own peer-owned caller; it has no private endpoint and no launcher-side caller. | ACP is the wrapper-to-Grok control protocol; MCP is the product-facing Sessionbus tool boundary. Caller state lives in the resident owner in both modes, never in a transient action hop. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt sends one ACP `session/cancel` notification; `{}` means the notification was sent, not that the run has stopped. The worker kit's universal close path interrupts an active lane run and waits for its terminal before invoking Grok's `close`, which releases the primary, observer, leader, private socket, and lock. In peer mode helper EOF releases its observer and peer, while launcher shutdown joins the TUI, quiet hold, and leader. | Grok adds no close timer: the daemon's single 10-second `closeBound` closes the worker and kills its process group if lane cleanup stalls. Grok's close callback never receives an active run; peer processes follow the product-owned stdio and launcher lifetimes. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open fields: **none**. Passive lane-delivery FIFOs are not permitted. | Grok exposes active interjection and all typed open controls; idle delivery starts a native turn through the shared Worker. |
| Size cap | **975 production / 930 test logical lines**, including ACP framing, both launcher modes, helper-owned peer mode, and leader bootstrap but excluding the shared wrapper host. | The measured implementation is 968 / 930. The peer launcher owns only leader, quiet hold, TUI, and argument translation; the product-spawned helper owns peer, caller, in-process tools, and its lazy observer. |
| Deletion inventory | Delete all `internal/products/grok` (2 files / 637 lines), `internal/launcher/{grok_peer.go,grok_peer_test.go}` (2 / 1,183), `9f366be:cmd/agent-sessions/grok_peer.go` (1 / 213), and all 11 `internal/bridge/grok*.go` files (1,883). `internal/launcher/lane_grok_test.go` is counted once in the shared row. Rewrite `grok/.mcp.json` and `grok/scripts/native-entry` as dual-entry peer/direct or lane/local assets. Delete the disproven launcher-owned peer, caller, private peer endpoint, roster observer, artifact watcher, and `/new` Replace path. Total legacy deletion: **16 files / 3,916 lines**. | The wrapper receives copied, product-owned ACP/leader/observer slices rather than retaining a cross-product bridge package. The interactive launcher wraps the TUI only to own its private leader and quiet hold; product-spawned helpers own immutable per-session presence. |

### 4.5 Qwen Code

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | `qwen-peer` with a launch token owns one `qwen --acp` child and one ACP client for the lane session. Without a token, `qwen-peer` mints a Qwen-compatible v4 UUID, passes it to Qwen as both `--session-id` and `SESSIONBUS_SESSION_ID`, then execs interactive Qwen; its spawned stdio MCP server owns the peer connection. | `internal/products/qwen/lane.go:102-181` proves the headless ACP lifetime. A hand-started peer lacking this identity returns the launcher error and never hellos; obsolete file-observer presence dies. |
| Open and resume | Initialize ACP v1, mint a v4 ID for fresh open and pass it in `_meta["qwen-code/sessionId"]` to `session/new`, or use capability-checked `session/resume` with `resume_session_id`; verify and return the exact product ID, then rename fresh sessions to the composed title. Supported open fields are `cwd`, `permission_mode`, `model`, and `arguments`; model maps to `-m`. Default permission uses Qwen's ordinary mode; bypass adds `--yolo` and verifies the returned mode. Arguments may not claim `--acp`, approval/yolo, resume/continue/session-id, prompt/input/output, or name controls. | Qwen Code 0.23.0 accepts the session ID metadata, resume, and `-m`; it exposes no reasoning-effort flag or ACP field. The wrapper mints only because Qwen requires the caller-provided UUID and fails closed on reserved controls. |
| Run | Start `session/prompt`, accumulate session updates, and resolve the matching future to a terminal result. | `lane.go:199-263` and `client.go` prove the one ACP request/future. |
| Tools | In both modes ACP `mcpServers` starts stateless `qwen-peer mcp` calls against the resident wrapper's private Unix endpoint; the wrapper owns the caller and its peer or worker connection. | c5 already injects an MCP server during `session/new` (`lane.go:134`); the product-spawned helper remains the tool entry but owns no connection or caller state. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt calls `craft/cancelPendingPrompt`; close cancels the ACP lifetime and reaps the child. | `lane.go:265-310` proves both calls. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open field: `reasoning_effort`. Native mid-turn drain state remains product-owned; no passive lane FIFO may replace automatic processing. | Product help exposes model but no effort selector; permission vocabulary and reserved arguments stay wrapper data, and no Qwen condition enters the daemon. |
| Size cap | **520 production / 600 test logical lines**, including ACP framing but excluding the shared wrapper host. | The current driver/client split contains generic actor state that disappears; all retained Qwen protocol code remains charged here. |
| Deletion inventory | Delete all `internal/products/qwen` (4 files / 1,031 lines), `internal/launcher/{qwen_peer.go,qwen_peer_test.go,qwen_test_helpers_test.go}` (3 / 1,412), `9f366be:cmd/agent-sessions/qwen_peer.go` and `9f366be:cmd/agent-sessions/qwen_peer_test.go` (2 / 234), and the obsolete 11-line `qwen/scripts/native-entry`; replace it with the installed dual-entry MCP image and rewrite `qwen/mcp.json`. Total: **10 files / 2,688 lines**. | ACP becomes lane-wrapper-owned; event-file identity and old peer launcher state die, while a thin peer exec plan and product-spawned MCP entry replace them. |

### 4.6 OpenCode and Kilo

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Worker entry | Token-selected `opencode-peer` or `kilo-peer` is only a boot shim for `<product> serve --hostname 127.0.0.1 --port 0`; the in-process JavaScript plugin links the native worker kit and owns the one daemon connection. Without a token the entry launches the interactive product and the same plugin runs peer mode. The shim remains only until upstream product boot checks the token directly. | The product SDK already owns session creation, prompt, events, abort, title, tools, and directory. Keeping the universal lifecycle in the plugin makes this the DSH-native shape rather than a Go HTTP adapter. |
| Open and resume | After a v2 SDK capability probe and app-ready, create or fetch the exact product session, return its ID, apply the composed title and permission rules, and retain model/agent/variant defaults. Both products support all five open fields; ordered arguments allow only the documented `--agent` selector. | `opencodefamily/lane.go:69-187` proves the product primitives. The deployed pdev plugin/SDK is 1.2.10 while the CLI is 1.18.28, so the v2 probe must succeed before hello rather than trusting the CLI version. |
| Run | Call `session.promptAsync`, follow the exact event stream, then fetch the matching assistant result. | `lane.go:168-337` and `client.go` contain the existing bounded HTTP/SSE primitive. |
| Tools | The plugin's peer and worker modes use the same JS caller/worker kit and register the same product tool; there is no lane-local bridge endpoint. | The current plugins already own SDK tool registration in `9f366be:integrations/opencode/agent-sessions.mjs` and `9f366be:integrations/kilo/agent-sessions.mjs`; the token changes hello mode, not transport shape. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt calls the SDK abort endpoint and cancels event wait; close disposes the exact product session and lets the kit close the socket. | The plugin owns both session and connection, so no private server supervisor state enters the bus. |
| Exception ledger | Section 1 code exceptions: **0** for both products. Declared unsupported open fields: **none**. Product lifecycle exceptions: **0** once the v2 SDK and delivery probes pass. | Dialect differences remain product SDK data; they never select a wire method or daemon branch. |
| Size cap | Shared native plugin **750 production / 700 test**, plus **60 / 80** per boot-shim/dialect leaf, excluding the shared JS kit. | OpenCode and Kilo differ only in SDK dialect, permission mapping, and packaged entrypoint; product transport remains charged to this family. |
| Deletion inventory | Delete all `internal/products/opencodefamily` (10 files / 2,776 lines), `internal/products/opencode` (2 / 93), and `internal/products/kilocode` (2 / 98): **14 files / 2,967 lines**. Rewrite, but do not duplicate or delete, the two retained integration packages and tests. | Server pooling, daemon driver maps, doctor probes, and Go leaf drivers are replaced by one native JS plugin family plus two boot shims. |

### 4.7 Pi

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | `pi-peer` with a launch token owns one exact `pi --extension <managed-plugin> --mode rpc ...` JSONL child for product `pi`. Without a token, `pi-peer` launches interactive Pi, whose JavaScript extension owns the direct peer connection through the JS kit. | `internal/products/pifamily/lane.go:119-164` is the proven lane RPC launch; interactive Pi already owns the extension lifecycle. |
| Open and resume | Fresh mints a Pi-compatible ID, passes `--session-id <id>` and the composed product title, then returns the product state ID; resume passes `--session <resume_session_id>` and verifies the returned state. Pi supports all five open fields: model maps to `--model` and reasoning effort to independent `--thinking` before child spawn. | `pifamily/lane.go:76-173` and `quirks.go:113-134` prove the transaction; Pi product help exposes the exact session, model, and thinking flags. |
| Run | Send RPC `prompt`, observe terminal JSONL events, and read the final assistant text. | `pifamily/lane.go:179-300` and `rpc.go` are the existing primitive. |
| Tools | The retained Pi extension runs in peer mode with a direct JS-kit daemon connection, or in lane-local mode against the wrapper's private endpoint; both register the same caller tool. | `integrations/pi/pifamily.mjs:106-151` already owns product tool registration; only the selected connection mode changes. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | Interrupt sends RPC `abort`; close reaps the exact RPC process while leaving its transcript durable. | `pifamily/lane.go:331-379` proves both operations. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open fields: **none**. Passive lane-delivery FIFOs are not permitted. | Model and thinking are process flags applied after open; idle delivery starts work through the shared Worker. |
| Size cap | Shared Pi-family wrapper **650 production / 700 test**, plus Pi leaf **60 / 80**; includes JSONL framing and excludes only shared wrapper-host code. | Product quirks are fixed launch/terminal data, not lifecycle branches; native framing is not an uncounted utility. |
| Deletion inventory | Delete all `internal/products/pifamily` (8 files / 2,242 lines) and `internal/products/pi` (3 / 110): **11 files / 2,352 lines**. Rewrite `integrations/pi/pifamily.mjs` and its test as a local wrapper plugin; retain the package entrypoint/manifest. | The common driver and doctor disappear; the family wrapper owns the same native RPC with no daemon-facing interface. |

### 4.8 OMP

| Ledger item | Decision | Source-backed reason |
| --- | --- | --- |
| Resident wrapper | `omp-peer` with a launch token owns the exact `omp --extension=<managed-plugin> --mode=rpc ...` JSONL child for product `omp`; resume is spelled `--resume=<id>`. Without a token, `omp-peer` launches interactive OMP, whose JavaScript plugin owns the direct peer connection through the JS kit. | `internal/products/pifamily/quirks.go` and `rpc_lane_test.go:388-437` prove the equals-style lane dialect; interactive OMP already supervises its plugin. |
| Open and resume | Create or resume the exact OMP session, call `set_session_name` at fresh open so the product title equals the composed bus name, and apply mapped permissions. OMP supports all five typed open fields: cwd maps to `--cwd=`, model to `--model=`, and reasoning effort to `--thinking`. Its three documented extra arguments are `--tools`, `--exclude-tools`, and `--approval-mode`; conflicts with typed permission fail before spawn. | OMP product help exposes the typed flags, while its ready/RPC surface proves the title call. The default permission path fails closed when RPC approval mediation is unavailable; bypass maps explicitly to the product's noninteractive mode. |
| Run | Send RPC `prompt`, accept OMP's declared terminal event, and read final assistant text through the family implementation. | `pifamily/rpc.go` contains the closed event decoder; OMP selects the terminal quirk rather than a second lifecycle. |
| Tools | The retained OMP entrypoint loads the Pi-family plugin in peer/direct or lane/local mode and registers the same caller tool. | `9f366be:integrations/omp/agent-sessions.mjs` is already a three-line family entrypoint; the shared plugin owns mode selection. |
| Deliver | Idle delivery starts native work. Lane mode receives a full delivery seed through the shared Worker Run; interactive mode uses the product-owned native wake path. Active delivery must meet Section 1's required active-work delivery contract; continuation after completion is only completion-race recovery, not a substitute for mid-task delivery. Report only the demonstrated admission boundary. | The message itself requests work. Product-native transport and admission details are recorded in the sessionbus-peers product ledger; passive FIFO storage until a later explicit prompt is not conforming. |
| Interrupt and close | RPC `abort` and exact process cleanup are identical to Pi. | No OMP-specific lifecycle callback is justified. |
| Exception ledger | Section 1 code exceptions: **0**. Declared unsupported open fields: **none**. Passive lane-delivery FIFOs are not permitted. | OMP exposes every open value as a process flag; its dialect remains launch/result data only and never reaches the daemon or wire. |
| Size cap | OMP leaf **60 production / 100 test** in addition to the shared Pi-family cap. | The leaf may declare quirks and permissions only. |
| Deletion inventory | Delete all `internal/products/omp` (3 files / 135 lines); shared `pifamily` deletion is counted under Pi. Rewrite the retained OMP entrypoint/test/manifest around the local wrapper plugin. Total: **3 files / 135 lines**. | OMP becomes one immutable family-wrapper registration, not a driver package. |

### 4.9 Wrapper migration ledger

| Ledger | Files | Deleted lines | Decision |
| --- | ---: | ---: | --- |
| Product-specific rows in Sections 4.2-4.8 | 69 | 14,743 | Delete every non-native daemon driver, bridge-owned product slice, and product-specific depart-on-exec launcher. |
| Shared wrapper boundary in Section 4.1 | 18 | 2,532 | Delete the remaining launcher package and connector self-refresh; retain only rewritten CLI/wrapper entrypoints. |
| Section 4 subtotal | **87** | **17,275** | Replacement wrapper and test lines are reported separately against the stated caps. |
| Cumulative Sections 2-4 floor | **150** | **31,042** | This is the minimum physical deletion from c5b280d before Section 5 migration/conformance cleanup. |

## 5. Migration and conformance

### 5.1 State boundary

The universal daemon reads only a new, initially empty table file; it never
reads, migrates, backs up, or deletes the c5 catalog file, which remains wholly
owner-controlled.

### 5.2 Ordered implementation and runtime proof

| Phase | Source deliverable | Gate | Runtime rule |
| ---: | --- | --- | --- |
| 0 | Signed document, `bus/sdk/go/protocol` schema and shared fixtures exported by the public SDKs, generated protocol, architecture boundaries, independent SDK module, SPDX coverage, and deletion ledger. | Schema fixtures pass in Go and JavaScript; method/error tables are byte-identical; deletion counts reproduce from c5b280d; plain root gates use its one relative SDK replace and SDK `GOWORK=off` gates pass without a replace; `bus/` has no wrapper import or product token. | No installed daemon or product runtime. |
| 1 | `bus/sdk/go` worker kit, universal daemon, durable lane table, Go caller kit, reference caller, and token-selected `bus/cmd/example-peer` reference worker. | Daemon caps hold; unit/race/vet/build green; an in-process restart with durable rows proves every row loads offline with empty maps/reservations and no spawn; old actor/driver/control packages are absent; contract learned nothing from adapters: **yes**. | Installed-daemon integration runs only on `umka-dev1`, against an empty universal table. |
| 2 | JavaScript worker kit, JavaScript caller kit, then the unified DSH plugin/profile. | All 17 shared lifecycle fixtures pass in both worker kits; DSH passes both cells in Section 5.5. | DSH installed-product proof only on `umka-dev1`; no other product is enabled. |
| 3 | `wrappers/` products in order: Claude, Codex, Grok, Qwen, OpenCode, Kilo, Pi, OMP; each product lands lane mode first and its shared-MCP entry second. | Each product meets its size/exception ledger and passes its two conformance cells before the next product is enabled. | Product runtime proof only on `umka-dev1`; failures do not enable a compatibility path. |
| 4 | CLI rendering, package projections, install/remove inventory, documentation, and federation. | Full unit/race/vet/build/package gates; federation assertions follow Section 5.7; all 18 cells pass in one clean candidate run. | The sole full runtime matrix runs on `umka-dev1`; no install elsewhere. |
| 5 | Release candidate. | Universal state starts empty; no old protocol endpoint, actor, driver, launcher process, socket, or compatibility package remains; the c5 catalog file's SHA-256 is recorded before and after the install and full run on `umka-dev1` and must be identical. | Production installation requires owner authorization after the clean `umka-dev1` evidence is sealed. |

Source compilation and unit/race tests, including in-process daemons and workers
on temporary sockets, may run in any isolated clone. Installing or running the
daemon, a wrapper, a product, a TUI, or a conformance cell as a service or
against installed products is permitted only on `umka-dev1`.

### 5.3 Reference sides

| Reference | Closed behavior |
| --- | --- |
| Reference worker | PATH-resolved `example-peer` starts with the one-use launch token in its environment and empty argv. Its hello declares all five open fields. Ordered `open.arguments` entries are `key=value`; `session_id=<id>` selects the returned product session ID and its absence makes the worker mint one. A plain turn input is echoed; `block` waits only for the run cancellation and returns `interrupted`; `call <method> <params-json>` performs that worker-originated session method and returns the response JSON; `fail <code>` returns that error. An idle delivery seeds a Run and is echoed without any further prompt; a delivery during a run is `injected` and appended to the echoed result. The close callback returns immediately. The worker has no configuration, clock, or product import. |
| Reference caller | `bus/cmd/sessionbus-call [-name <name>] [-g a,b] [-socket <path>] <method> [<params-json>]` opens one peer connection, sends exactly one raw client-to-daemon request, prints its result or error object on stdout, and exits 0 for a result, 1 for an error, or 2 for usage. `turn.run` waits for its terminal; deliveries during the call are JSON lines on stderr and receive `injected`. Separate invocations supply a second peer or abandon a reply sink. The binary has no configuration file, stdin protocol, product condition, or local turn registry. |
| Worker invocation | The worker suite accepts only a product token and invokes that exact PATH binary with empty argv and a launch token in its environment; every vendor and `example-peer` run the identical trace. |
| Caller invocation | The caller suite invokes the product's installed Sessionbus tool, not a private test API; every product runs the identical trace against the reference worker. |

The caller/reference size contract is final logical lines:

| Reference surface | Production | Tests |
| --- | ---: | ---: |
| Go caller kit | 250 | 400 |
| Go no-hello client (`client.go`) | 30 | — |
| JavaScript caller kit, additional to the 200-line JavaScript client/worker cap | 150 | 150 |
| Reference caller | 200 | 200 |
| Reference worker | 250 | 150 |

A caller kit over its cap has become a second router and is a design finding,
not a reason to move lines into a product integration.

### 5.4 Vendor acceptance traces

| Trace | Caller against reference worker |
| --- | --- |
| C1 | Two peer connections prove exact session identity, visibility-filtered list, and the full tool schema; the second visible peer can run, interrupt, and close the lane. |
| C2 | Describe without open or residue; two-level spawn composes matching name/private-group paths, gives each lane only its parent and own private groups plus `extra_groups`, and proves explicit ancestor-group widening; all declared open fields and ordered arguments survive. Discovery advertises configured product names without gating an unlisted executable and a listed-but-missing name returns `unknown_product`; explicit `host` naming the other daemon in a two-daemon fixture describes and creates the row only there, while an unfederated host returns `unknown_host`. |
| C3 | Wire start returns canonical RunRef; status and bounded wait are non-consuming reads of the resident Worker cursor. Replacement callers and cancellation retain output until ack or retirement. |
| C4 | Wait returns validated native terminal or explicit unavailable. Oldest-first ack commits consumption; pre-cancelled ack writes nothing and submitted ack settles. Worker retirement invalidates the cursor; no daemon body store or restart recovery. |
| C5 | Send resolves ID/name/group, deduplicates, and returns dispositions `written`, `injected`, `queued_for_next_turn`, or `rejected`, including exact rejected reasons `ambiguous` and `no_receipt`; an invisible peer receives `unknown_session`. |
| C6 | Concurrent interrupts coalesce to one worker interrupt and idle interrupt maps `not_running`. |
| C7 | Owner cleanup and terminal auto-close are independent and use ordinary Close. Open arms no deadline; admission cancels grace and proven refusal restores it. One 10-second close bound includes deferred execute acknowledgment, admitted-reader drain and reap; expiry invents no native terminal. Row remains resumable unless forget. |
| C8 | Peer EOF reconnects; a new connection with the same canonical ID supersedes the displaced identity terminally. Same-ID re-hello refreshes name/info with fixed groups. Different-ID re-hello atomically detaches the old reply sinks, fails its pending deliveries once, removes its private group, and installs the new identity/group before acknowledgement; requests admitted before the switch retain the old source. |
| C9 | The caller drives the remote row by canonical ID; offline resume replays the stored open value unchanged with argument order preserved; `name_taken`, `already_connected`, `not_connected`, forgotten-row `unknown_session`, disconnected-host `unknown_host`, and ambiguous one-hop loss `forward_lost` match Section 1.4; cleanup leaves no connection, process, token, or pending call. |

| Trace | Worker against reference caller |
| --- | --- |
| W1 | Describe launch reads the endpoint, consumes and scrubs the token and reserved local-key value, rejects that value when nonempty, sends one valid hello after app-ready when it is empty, never opens or closes natively, and exits on EOF without reconnect. |
| W2 | Fresh and resumed open return the exact product session ID; resume mismatch, unsupported field or value, typed-field/argument conflict, duplicate session ID, exit, and timeout fail truthfully. |
| W3 | Worker-originated client-to-daemon session methods are `not_committed` before open commit; after a successful open response, already-read later frames are withheld from dispatch until the durable commit and then succeed without a kit-side wait. Commit failure closes the provisional connection. |
| W4 | Completed, interrupted, failed, and empty-output native results produce the exact terminal shapes; over-limit native output produces `unavailable`; a second run is busy. |
| W5 | Reader remains full-duplex: delivery and a worker-originated session method complete while run is blocked; delivery receipts are exact. |
| W6 | Concurrent interrupt and close invoke one native interrupt; terminal-before-interrupt invokes none. |
| W7 | Close claims the slot before product code, rejects later delivery as closing, writes the one run response from its run owner, and completes interrupt/terminal/close-response/socket-close/`closed` ordering within the daemon's one 10-second `closeBound`; an unresponsive callback is killed at that bound. |
| W8 | Control EOF cancels every callback, calls native close once, fails pending calls once, and exits; later explicit resume creates one new worker process. |
| W9 | Malformed, unknown, oversized-frame, overlength-payload, duplicate-member, and out-of-range-ID inputs follow Section 1.1; product callbacks never see rejected frames and cleanup leaves zero residue. |

### 5.5 Eighteen-cell matrix

| Product | Caller cell against token-selected `example-peer` | Worker cell against reference caller |
| --- | --- | --- |
| DSH | `C-DSH`: C1-C9 through the unified DSH plugin's registered tool. | `W-DSH`: W1-W9 against token-selected `dashi`. |
| Claude | `C-Claude`: C1-C9 through the product-spawned peer MCP entry. | `W-Claude`: W1-W9 against token-selected `claude-peer`. |
| Codex | `C-Codex`: C1-C9 through the product-spawned peer MCP entry. | `W-Codex`: W1-W9 against token-selected `codex-peer`. |
| Grok | `C-Grok`: C1-C9 through the product-spawned peer MCP entry. | `W-Grok`: W1-W9 against token-selected `grok-peer`. |
| Qwen | `C-Qwen`: C1-C9 through the product-spawned peer MCP entry. | `W-Qwen`: W1-W9 against token-selected `qwen-peer`. |
| OpenCode | `C-OpenCode`: C1-C9 through the JavaScript plugin in peer mode. | `W-OpenCode`: W1-W9 against token-selected `opencode-peer`. |
| Kilo | `C-Kilo`: C1-C9 through the JavaScript plugin in peer mode. | `W-Kilo`: W1-W9 against token-selected `kilo-peer`. |
| Pi | `C-Pi`: C1-C9 through the Pi-family plugin in peer mode. | `W-Pi`: W1-W9 against token-selected `pi-peer`. |
| OMP | `C-OMP`: C1-C9 through the Pi-family plugin in peer mode. | `W-OMP`: W1-W9 against token-selected `omp-peer`. |

All eighteen cells are required. A failure may change only the failing product
wrapper/plugin or a genuinely universal kit defect reproduced by token-selected
`example-peer`; it may not add a product branch, capability exception, or
alternate daemon path. Every cell records **contract learned nothing from the
adapter: yes/no**, and any `no` fails the candidate.

### 5.6 Upstream native-worker feasibility

The target for OpenCode, Kilo, Pi, and OMP is a plugin-native worker inside the
product's own headless start, with no Go wrapper. The deciding current product
facts are:

| Product | Session-level cwd/model fact |
| --- | --- |
| OpenCode | **Present:** its plugin SDK accepts `directory` on session operations and a model object on prompt operations, so both values can arrive after the process and plugin start. |
| Kilo | **Present:** its plugin SDK accepts `directory` on session operations and carries the active prompt model through its session APIs, so process start need not fix either value. |
| Pi | **Absent today:** the extension context exposes the process cwd and session manager, but not the proposed upstream `session.configure({cwd,model})` primitive; both values remain process flags. |
| OMP | **Absent today:** the shared extension context likewise lacks the proposed upstream `session.configure({cwd,model})` primitive; both values remain process flags. |

These facts affect only when a vendor can replace its wrapper with the native
kit. They do not change Section 1, add daemon capabilities, or excuse a wrapper
from the same conformance matrix.

Before its conformance cells, each product has a named `umka-dev1` probe:

Every product probe also kills a running wrapper or native worker abruptly and
attempts immediate resume. Resume must fail as `spawn_failed` with `session
busy` until the still-running native writer exits, then succeed. This proves
cross-process native-session exclusivity rather than an in-memory duplicate
check.

| Product | Required installed-product probe |
| --- | --- |
| DSH | Profile composition, global Cordis patch/package resolution, app-ready timing, one connection per root, title/append/steer/terminal reason, and bounded supervisor exposure around `whenIdle`. |
| Claude | `system/init` timing relative to the first stream input, `--mcp-config` precedence, session-ID/title flags, permission `dontAsk`, active injection, private Unix MCP, and `/clear` followed by same-socket different-ID re-hello with only the new titled peer visible. |
| Codex | Deferred `_meta.threadId` peer identity, per-thread `mcp_servers.<id>.url`, thread naming, approval/sandbox mappings, steer, interrupt, resume, and `/clear` followed by first-call different-ID re-hello with only the new titled peer visible and no `inactive` state. |
| Grok | Private leader startup, product-ID allocation through `session/new`, exact `session/load` resume, provisional-lock rename, observer rename/interjection/held-queue acknowledgements, default-leader peer delivery, and cleanup under the daemon's `closeBound`. |
| Qwen | ACP resume and `_meta["qwen-code/sessionId"]`, rename, permission vocabulary, `craft/drainMidTurnQueue` including undrained recovery and its 2-second/three-strike/`-32601`/30-second bounds, and private Unix MCP. |
| OpenCode | v2 SDK directory/model/title/prompt/abort/delivery primitives before hello, plus the installed SDK/CLI version pair. |
| Kilo | The same v2 SDK probe as OpenCode; runtime support remains explicitly unproven until Kilo is installed on `umka-dev1`. |
| Pi | Exact `pi --extension <path> --mode rpc` launch, product ID, fresh/resume ID, title, model/thinking flags, steer, abort, and terminal event. |
| OMP | Exact equals-style child invocation including `--resume=`, product ID, `set_session_name`, fail-closed permission behavior, three extra arguments, native `nextTurn` peer queue, steer, abort, and terminal event. |

### 5.7 Federation gate

| Gate | Assertion |
| --- | --- |
| Identity | Every local and remote summary emits the same canonical `id@host` and optional `name@host`; no separate host field or receiving-side relabeling exists. A standalone daemon uses `local`, and a federated daemon requires a configured non-`local` unique host name. |
| Visibility | The destination daemon resolves and filters its own current directory with the carried admitted groups. The origin and hub keep no remote row. |
| Messaging | One canonical remote message is forwarded once, produces one receipt, and is never retried or duplicated after federation reconnect. Bare input selects the caller's own host; qualified input is split only at the last `@`. |
| Control and creation | Canonical remote resume/run/interrupt/close and explicit-host spawn/describe travel exactly one hop as `{from:{session_id:id@host,name:name@host,product,private_group,groups},request}`; the TLS connection identifies the origin and the JSON-RPC ID correlates it. The destination enters the same local request dispatcher with that captured identity, rejects a non-local target, and never forwards again. Collector loss removes its reply sink; authenticated lifetime loss separately closes nonpersistent children. Transport loss returns `forward_lost` without retry. |
| Federation authentication | `sessionbus secret` produces 32 random bytes in base64; either side rejects a decoded secret shorter than 32 bytes. Wrong secret, unknown or reserved `local` name, or name/key mismatch fails TLS. The registry admits the first authenticated attachment and closes a later duplicate host, preserving the incumbent. Config is immutable while running; rotation restarts both endpoints and reconnects. No separate expiry/revocation exists, and secret-bearing config is mode 0600. |
| Optional local encryption | **Not implemented in 0.5.0; reserved.** Both kits consume and scrub `SESSIONBUS_LOCAL_KEY` and reject a nonempty value. The passing 0.5.0 rows cover the owner-checked `0700` runtime directory, `0600` socket, and launch-token scrubbing—not a local-TLS handshake. Host-to-hub TLS remains implemented and required. |
| Roster and aggregate list | `federation.hosts` returns one sorted snapshot of current authenticated host names. An aggregate list fans out from that capture and fails wholly with `forward_lost` if any captured leg is lost; there is no atomic cross-host list or partial result. |
| Reconnect | The 256 pending-forward cap is additional to conn bounds; admitting the 257th closes that destination link. Disconnect fails admitted destination calls once as `forward_lost`. The configured attachment is one-shot: later loss requires daemon restart, whose fresh attachment and ID space never receive an old reply or replayed request. |
| Refused federation machinery | The hub has no replicated session/product state, notifications, replay, durable rows, distributed locks, automatic placement, multi-hop routing, or capability negotiation that gates PATH launch. |

### 5.8 Test rehoming

| Deleted test family | Universal owner |
| --- | --- |
| Daemon lane actors, registries, projections, collectors, archives, timers, product dispatch, and argv reparse | Router/table tests drive the eighteen methods over a real connection and assert rows, current admissions, pending calls, lifetime ownership, independent deadlines, and supervisor cleanup. |
| Presence, messaging, federation, roster, names, and notices | Daemon visibility/resolution tests plus the federation gate; no test constructs a private actor or product driver. |
| Product lane drivers and peer launchers | Each Section 4 wrapper test drives its six callbacks and exact native transcript; the shared wrapper-host unit suite proves bounded active delivery and automatic turn-boundary handoff, stale lock files do not block, a live inherited flock survives wrapper death until the child exits, interrupt at native-turn creation, first-turn/terminal delivery handoff races, and child death with unresolved native delivery invents no receipt while leaving the row resumable; peer exec-plan tests stop at product config and never claim socket ownership. |
| Go/JavaScript lifecycle duplication | The one 19-row fixture table runs unchanged through both native kits and the reference worker. |
| Connector and plugin tool tests | Caller-kit conformance C1-C9 through the installed peer MCP/plugin entry, with product-private transport tested only at its local boundary. |
| Packaging and release projections | Package tests assert one schema/kit projection, correct peer and lane entry forms, no deleted compatibility artifact, and byte-identical installed assets. |
| Protocol and design documentation | Generate `bus/docs/PROTOCOL.md` from Sections 1 and 3.1 verbatim, with this document as the sole source and `bus/sdk/go/protocol` as the sole schema/fixture authority consumed by both public SDKs. The npm package includes the runtime schema and both suites enforce byte identity; no generated copy may drift. Delete every superseded lane-convergence, presence-supersession, adapter-boundary, and DSH-adapter note under `docs/designs`; do not retain archived or paraphrased protocol authorities. |

Rehoming is mandatory proof, not permission to preserve a deleted abstraction
under a new test helper. Every deleted test is named in the implementation
ledger beside its replacement row.
