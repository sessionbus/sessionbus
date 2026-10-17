# Same-user operator roster

This v0.5.1 amendment restores the operator inventory omitted when the legacy
admin channel was removed. It supersedes the universal design's assertion that
ordinary `session.list` replaces all operator diagnostics. The public protocol,
SDK methods, group visibility, and native product ownership remain unchanged.

`sessionbus roster [--all] [--json] [--local] [--socket PUBLIC_SOCKET] [--timeout 15s]`

The CLI displays rows with `connected || running` by default. `--all` includes
offline and retained/archived entries. Filtering applies to local and remote
rows in table and JSON output, after decoding the operator response. The wire
inventory and its bounds are unchanged; host errors and completeness remain
visible even when all of a host's rows are hidden.

The command uses a separate mode-0600 Unix endpoint in the daemon's existing owner-validated
mode-0700 runtime directory. Its name is `op-<first 16 SHA256 bytes of public basename as hex>.sock`,
shorter than the mandatory derived lane endpoint. Only stale sockets are
removed; ordinary files, symlinks, and active endpoints are rejected. Closing
the daemon closes, cancels, and joins accepted operator connections. There are
at most eight admitted operator connections and each request has a deadline.

The endpoint accepts one JSON line `{ "local": false }` and returns one bounded
`sessionbus.roster.v1` report. It takes a coherent copy of local directory
metadata under the directory mutex. It includes connected peers and retained
lanes across groups, and never registers an observer. The explicit projection
excludes `info`, Open arguments, payloads, results, secrets, and lifetime tokens.
Lane requested permission mode is reported without inferring native approval.
Responses are bounded; errors never silently truncate a successful inventory.

Federation capability is negotiated using TLS ALPN `sessionbus-roster/1`,
also included in the newer `sessionbus-trace/1` and `sessionbus-wake/1` capabilities.
Existing pinned-key authentication is unchanged. New and legacy peers negotiate
no capability when either side lacks it. A new daemon uses the old hosts query
to report unavailable metadata on an old hub, without sending unsupported RPC.
A new hub reports unavailable metadata for old hosts without querying them.

The private `federation.roster` method is accepted only on the authenticated
host/hub connection, never on a public session connection or public forwarded
request. The hub captures its live attachment set, requests one local snapshot
per capable remote host, and returns the bounded aggregate to the same origin
attachment. It stores no replicated session roster. Requests have a five-second
aggregate bound, an eight-operation per-origin limit, and bounded destination
correlation. All connected, authenticated hosts share this diagnostic trust;
this is operator metadata, not an additional messaging permission.

The hub also sends the same private request for a second purpose: a link-local
check of a roster-capable host while a short call to it waits (see
the protocol's federation lifetime controls). That check uses no origin
operator slot and no per-origin operation, keeps at most one request
outstanding per link, and follows its own rule: after 10 s of waiting and
inbound silence it is sent, and if no frame arrives within 20 s after the later
of that request and the host's last inbound frame, the hub closes the link. Any
reply, including an error, proves the link answered and clears the check;
responses remain snapshots either way.

Local and remote snapshots are observations at different instants. Loss,
unsupported capability, excessive size, or timeout produces explicit incomplete
status and a nonzero CLI exit. `--local` allows local inspection independently
of federation. Human output replaces terminal control characters; JSON retains
the original metadata strings. The JSON schema identifier is independent of the
legacy roster projection because the generic daemon no longer
owns native permission or product-specific status fields.
