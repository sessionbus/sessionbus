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
Active delivery keeps the ordinary native admission path and Run token. It must
be consumed by that turn or automatically continued; no human prompt is needed.
At the final native handoff, the product synchronizes delivery with native turn
completion. If the turn ended, or the product has no source-proven automatic
active-delivery path, and nothing was written, steered or queued, it returns
`ProtocolError(-32004, not_running)`. Safe native mid-turn admission remains the
preferred path; this refusal must precede any native submission. The kit also returns this error
without invoking the product when the run is already absent or its context is
cancelled. The product check is still required: completion may race the kit's
check. Never return this error after native admission or an uncertain write.

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
- Delivery is mandatory. Products that cannot inject during a native run queue
  for the next turn in their wrapper and report that disposition; the daemon
  has no injection capability flag or queue.
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
| `open` | `spawn_failed` with `stderr_tail:[message]`; the daemon passes it through unchanged. |
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
