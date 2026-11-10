# Experimental hub federation policy

**EXPERIMENTAL: do not enable this on a real hub yet.** The relation-table
limitations below must be addressed before relying on it.

`sessionbus-hub --policy /absolute/path/policy.json` limits federation at the
hub. It does not restrict local sessions or change the host secret map. The
file must be a mode-0600 regular JSON file, at most 1 MiB:

```json
{"hosts":{"guest":{"groups":["team"],"lanes_on":[],"lanes_to":["workstation"]},"workstation":{"lanes_on":["guest"]}}}
```

Omitting the option leaves forwarding unchanged. An absent host entry or field
leaves that dimension unrestricted; an empty array permits nothing in that
dimension. Group and lane rules are independent. Names are literal, with no
globs. Group rules contain named groups, not internal `session:` groups.
Unknown, duplicate, null and malformed fields are refused. Every host key and
every name in `lanes_on` or `lanes_to` must occur in the loaded host secret map,
including known hosts that are currently offline. A typo is an error naming that
host; it cannot silently leave the intended host unrestricted. An explicitly missing
or invalid policy fails startup before opening the listener.

After replacing the file, send SIGHUP to the hub process to validate and install
the whole replacement atomically. A bad replacement logs one error and retains
the old policy. A successful applied reload logs its path and time once. SIGHUP
on a hub started without `--policy` is logged and ignored; it does not enable
policy or kill the hub. Reload does not restart the hub or cut connections. Host-secret
registration still follows its existing restart procedure.

## Groups and existing work

When either host has a groups rule, ordinary caller groups are reduced to named
groups permitted for both hosts. A request with no remaining named group and no
recorded lane relation is refused before dispatch. On ordinary requests, internal
groups are removed and the mandatory private group becomes a fresh random value
for that call. The receiving daemon then applies its ordinary shared-group check.
A group send must name a group permitted for both hosts. Local traffic is
unchanged.

The hub keeps no session-row cache and fetches no target row. Model-facing list
rows have non-allowed named groups removed. An unscoped list does not enumerate
private-only children when there are no allowed named groups; use their returned
IDs. Operator roster rows require an allowed named group and are otherwise
omitted whole, including their identity and product. The hub's private roster
probes remain complete and are never returned to another host.

The destination still decides what to do with a permitted call. A modified source
daemon can impersonate sessions on its own authenticated host, and a modified
destination controls delivery inside that host. The hub limits which groups and
lane relations cross it; it does not isolate processes inside a colleague's host.

## Remote lanes and relation lifetime

Fresh spawn, resume and describe require both destination membership in the
source's `lanes_to` and source membership in the destination's `lanes_on`.
Checks use the immediate authenticated hosts. If a lane on B spawns on C, B and
C's rules govern that call. Lane-rule changes do not interrupt existing Runs.

A successful forwarded fresh spawn involving a configured host records a
creator/lane relation. The hub computes its private groups from the verified
caller and requested name. Group-restricted ownership calls accept only the
caller's canonical own private group or its recorded derived lane identity,
plus allowed named groups. They never carry an inherited parent's private
group, even when another relation exists. Internal `extra_groups` may name only
that own verified private group; a borrowed parent group is refused. Recorded
relation groups cross only on ordinary explicit-counterpart calls.
Caller IDs containing `/` or an extra `@` in their local part cannot verify an
own private group for these restricted calls. Recorded lanes cannot resume an unrelated lane, even through allowed named
groups; a parent peer can do so. This avoids slash-name collisions between a
local leaf and a nested lane's derived private group. Their own recorded-child
resume/trace and fresh nested spawn remain available. No identity is rewritten
for ownership creation. Unrecorded local lanes therefore
use allowed named groups for ordinary federation but cannot create remote owners
through spawn, resume or trace configuration under a groups restriction.

Recorded bidirectional relations permit targeted creator/child calls after group
revocation. Trace configuration requires a recorded child. A trace copy is
allowed only to a recorded parent of a lane on the source host; the hub cannot
independently prove which of that host's lanes emitted a daemon trace copy.
Every explicit target in a batch must qualify for the relation exception.
Otherwise the entire batch uses the named-group rule; send a private-only child
separately. Broad lists and group sends do not acquire relation enumeration.

Close retains an entry for resume. Exact-ID forget removes only the captured
entry generation. Name-based forget relies on later roster cleanup. A complete
successful current-attachment roster whose request was selected after recording
may remove an absent lane; errors, incomplete replies, offline-retained rows and
older selections cannot prove absence. A modified host can retain its legitimate
entries by reporting rows. Each `(source, destination)` pair has at most 256 live plus in-flight relation
grants; a full pair returns `busy` without data, with no live-entry eviction.
Another source or the reverse direction has its own allowance. This replaces
the former destination-wide charge. Closed but not forgotten lanes still hold
slots. A destination without roster negotiation cannot provide roster pruning.
Authoritative target identity and grant recording precede public spawn success.
A late success after a silence answer can still record the grant, without a
second public answer or replay. Foreign, conflicting or stale-attachment success
is reported unconfirmed and cannot replace a grant.

The table is memory-only and empty after a hub restart. Enabling a groups rule
after previously untracked lane creation also cannot recover the original
creator relation; SIGHUP does not reconstruct history. Existing tracked entries
are retained when their rules change. Existing entries are
reused unchanged on resume, without adding another resumer. With no entry, a
successful resume whose forwarded groups were **only the verified canonical own
private group** proves deliberate access: creation or an explicit `extra_groups`
grant. It records directed caller-to-lane access for every targeted operation,
including Run/start, single-lane messages, collection, cleanup, trace configuration
and known-ID list. It proves no creator identity or derived group. Named groups
also forwarded mean no such proof and no new grant. This reconstruction consumes
the same live/in-flight cap before dispatch.

A directed entry never authorizes child-to-caller messages, completion notices,
reverse trace copies, transfers or derived-lane verification. After reconstruction,
reverse notices need independent allowed named groups; collect by polling. A
creator that was itself a lane cannot reconstruct its derived identity from an
empty table. There is no durable recovery or automatic resubmission.

Calls already admitted finish with their captured policy. A transfer reserves its
existing pending budget before capturing relevant immutable relation references;
the hub checks the actual bound endpoint IDs and groups once before adoption or
bytes. Bidirectional relations can qualify; directed resume access cannot. An
admitted transfer's chunks, finish and cleanup are not rechecked after reload.
Owner-end, host-end and liveness/hosts requests retain their normal cleanup and
transport behavior.

## Refusal and rollout

The new token is code `-32017`, message/reason `federation_policy_denied`.
Policy refusals do not cut a link and are not transport-loss answers. Destination
visibility failures retain their existing unknown answers. Transfers preserve
explicit `busy`, `unsupported_transfer` and `transfer_failed` refusals across the
bridge; lost responses, ended generations and post-create association failures
retain `internal` / `transfer_unavailable` uncertainty. Message delivery reasons remain arbitrary strings;
this adds one token without narrowing product-specific reasons.

A kit declares `info.sessionbus_policy:1` and receives `policy/1` only when that
hello opted in. Workers declare `supports_policy:true` only for daemon-issued
`sbt1:av1:fp1:` tokens. Old token hello bytes stay unchanged. Each daemon hop
negotiates the `sessionbus-policy/1` ALPN superset. Availability support alone is
insufficient. Old hops and non-opting kits get the existing `unknown_session`
answer or reason. Projection covers raw errors and trace envelopes, and strips
request-only bits from copied trace-owner and transfer-caller identities.

Update the hub first, then hosts and products in either order. An old hub supplies
no policy enforcement; an old daemon may display an unknown future kit Info key
as public metadata. The new Go and JavaScript SDKs declare the bit automatically;
raw connection owners must declare it themselves. No additional runtime or
package dependency is introduced.

## Product inventory and tracing

A host with limits sees another host's published products only when the two
share an allowed group or lanes are allowed from the listing host to that host.
A host that is not responding can still appear, marked not responding, as before.
A lanes-only pair forwards the same broad list with a fresh random private
identity and no named groups: the destination supplies its real products and
zero visible sessions. A responding pair allowing neither groups nor lanes
keeps the empty short-circuit. Published products are useful discovery, not
launch authority: direct describe/spawn still consult the destination's service
PATH.

At the existing trace-recipient reply filter, a recorded symmetric creator/lane
counterpart remains eligible. Other recipients across a restricted pair need a
named group allowed for both hosts. Kept entries retain their exact owner,
lifetime and required private identity group, with named groups clipped to the
allowed intersection. These named groups govern eligibility at that filter;
they do not add a new trace-copy submission path. Existing trace-parent and
exact target/lifetime checks still govern delivery. A reconstructed one-way
resume does not restore reverse trace. Restart or missing original tracking
therefore leaves only independent allowed named-group eligibility.

## Experimental limits and future work

An empty `{}` host entry still enables relation tracking and associated complete
roster observations while charges exist; it is not the same as omitting the
host. Ordinary liveness probes remain independently negotiated. Lanes-only and
unlisted pairs do not verify source-claimed session/private-group identity
through this relation table. A modified authenticated source can exploit that
existing identity gap; lane rules alone do not close it.

The policy matrix tests skip unless their environment is supplied. The retained
live matrix evidence was a one-off isolated acceptance run, not a default-CI or
fleet guarantee. No fleet hub is enabled by this release.

**Open next step before policy reliance:** redesign the relation table. The
proposal to replace it with verified canonical private-group forwarding still
needs a fresh check of the private-group bypass concern. Notice, nested-spawn
and restricted-trace trade-offs need a separate decision; no redesign is
implemented here. The duplicate error converter and four code/message tables
without a parity test remain recorded cleanup work.

## Shipped systemd user unit

The shipped unit has no policy environment fallback. Inspect its actual command
with `systemctl --user cat sessionbus-hub.service`, then create a drop-in with
`systemctl --user edit sessionbus-hub.service`. Clear `ExecStart`, copy the
installed command exactly, and append the absolute policy path. For example,
only when this is your actual installation prefix:

```ini
[Service]
ExecStart=
ExecStart=%h/.local/libexec/sessionbus/hub/current/bin/sessionbus-hub --policy %h/.config/sessionbus/policy.json
```

Keep the existing listener/config environment and any arguments your installed
command uses. Run `systemctl --user daemon-reload` and restart the user service
to adopt the changed command. This is an experimental development setup, not a
request to alter an existing real hub. Launchd uses its own `ProgramArguments`;
a systemd drop-in does not apply there.
