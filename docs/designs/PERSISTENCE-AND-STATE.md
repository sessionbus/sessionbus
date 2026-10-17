# IT IS A TRUSTED ENVIRONMENT. DOT.

Sessionbus connects the user's own agents on infrastructure they control.
Isolation belongs in deployment: separate VLANs or separate hubs. The host,
local socket, peers, and product adapters have no in-code trust boundary.

The only security item planned is a future daemon-to-hub app key so a hub can
admit known daemons. It is not designed today, has no preparatory hooks, and is
the entire security roadmap.

## Product authority

Products own session identity, titles, histories, cwd, models, permissions, results, and resume.
Sessionbus reads native events or product query surfaces and does not persist a copy. A lane
has exactly one identity: the product's native session ID.

Every invocation owns its launch facts. Start and resume receive groups, cwd, model, agent, effort,
permission, persistence, and auto-archive choices from that invocation only. Omission means no
override and the product applies its own default.

## The one durable table

Peers, messages, turns, results, names, process identity, live presence, and product metadata are
never persisted by Sessionbus. The sole durable record is an immutable offline-lane discovery
candidate:

- product and native session ID;
- historical parent ID and primary private group;
- assigned or inherited non-derived secondary groups; and
- optional parent host.

It is written idempotently when a fresh native identity is established and never rewritten on
resume or handover. Derived lane anchors are recomputed as `primary/native-id`; they are not stored.

The table answers only which UUIDs Sessionbus may ask a product about for a group-visible
caller. `list --all` and offline resume ask the product to confirm each eligible UUID and expose only
product-confirmed rows. A stale candidate yields nothing. Historical parentage is never presented
as current ownership.

## Live memory

An acknowledged protocol-v1 `session.hello` connection is the complete liveness proof. UUID, name,
groups, product, and info live in memory; EOF removes them. A newer same-UUID connection replaces
the older one. Name selectors may use a disposable UUID/name/product map, but groups and cwd never
live there as a second copy.

The daemon owns active lane actors, native driver handles, turn waiters, and current parent
ownership in memory. Nonpersistent idle lanes retire when their parent disconnects. A persistent
lane may become live and unowned, then attach to an eligible parent without creating a second native
session.

## Delivery, restart, and federation

Messages and turns are synchronous pass-through operations. Success is the recipient or product
acknowledgement. There is no mailbox, durable receipt, replay journal, or retry owner in the daemon.
Federation retains only accepted unacknowledged work and rosters in process memory.
The hub closes a roster-capable host link that stays silent while a short call to it waits (any call
except `turn.run`, `turn.wait`, `lane.spawn` and `session.close`): one `federation.roster` check after
10 s, close if no frame arrives within 20 s after the later of that check and the host's last inbound
frame. Idle links are never checked; hosts without roster capability rely on TCP.

After daemon replacement, daemon-owned lanes are non-live. Their product sessions remain available
through candidate filtering, product confirmation, and exact native resume. A client that owns its
presence connection may re-report after reconnect; the engine does not infer liveness from a row.

Graceful shutdown stops new admission, drains accepted work for at most two seconds, and exits.
