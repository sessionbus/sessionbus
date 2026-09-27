# Start with two sessions

This example connects two independently started Codex sessions on one machine.
There is no parent, managed lane or hub. The same communication contract also
works between different integrated products.

## Install

Use your normal login account. Install and authenticate native Codex first,
then install the Sessionbus host and Codex integration:

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/sessionbus/main/deploy/install-host.sh | sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/codex-peer/main/scripts/install-codex.sh | sh
```

You can download and inspect these scripts before running them. They select
published releases and verify their archive checksums. See the
[host installation instructions](README.md#install-binaries) and
[Codex integration installation instructions](https://github.com/sessionbus/codex-peer#install)
for platform requirements, update order and explicit version selection.
The peer installer does not install Codex or replace its login or permissions.

## Join the same group

Open two terminals in your project directory. In the first:

```sh
codex-peer -g sessionbus-demo -n bus-left
```

In the second:

```sh
codex-peer -g sessionbus-demo -n bus-right
```

The **same group on both commands matters**: ordinary discovery and messaging
require shared-group visibility. Use a different shared group if you already
have sessions in `sessionbus-demo`. A group is also an authorization boundary
for ordinary lane controls, not just a display label.

These are ordinary interactive sessions with the integration enabled. Keep
their normal native permission settings. Native trust or tool-approval prompts
still need to be handled in the native interface.

You can substitute another installed peer command, such as `claude-peer`, in
either terminal. Check the [Claude peer README](https://github.com/sessionbus/claude-peer)
for native naming, activation and delivery differences.

## Discover, then exchange a message

In each terminal, ask:

> Use the Sessionbus list action. Show my self_info.session_id and the other
> visible sessions with their IDs and names. In this turn only list; do not
> send a message. If you later receive a Sessionbus message asking you to
> reply, use Sessionbus to reply as requested, subject to native tool
> approval.

The two sessions should see each other. Use the IDs returned by the tool;
names may not appear until the native product publishes them. `self_info`
identifies the calling session even when the list is filtered.

In the left session, ask it to send a short message to the right session's
returned ID. For example, the public tool arguments are:

```json
{"action":"send","arguments":{"target":"RIGHT_SESSION_ID_FROM_LIST","message":"Hello from the other terminal. Please reply with your Sessionbus session ID."}}
```

Replace the placeholder; do not invent an ID. The right session can use the
message's actual source identity to reply through the same tool. A successful
exchange includes the reply, not merely a positive delivery receipt. Depending
on the native product and its current state, delivery is admitted into active
work or starts work when idle. Native queuing must process automatically without
another human prompt. A receipt never proves model consumption.

If no reply appears, inspect the receipt and the receiving terminal before
doing anything else. Do not automatically resend an uncertain delivery.

## If discovery is empty

- Check that both commands are the installed `*-peer` front doors and include
  the same `-g` value. Plain native launches do not necessarily enable the bus.
- Check native startup, trust and tool-permission prompts. Do not solve a denied
  tool call by changing permissions behind the user's back.
- Run `sessionbus roster --local` in a separate shell to inspect the daemon's
  operational view. This view covers all groups; a model's `list` is restricted
  to its visible groups.
- Confirm the daemon service and the native product are available in the
  normal login environment. An advertised product name is not an installation
  or authentication check.

Quit the two native sessions normally when finished. No managed lane was
created by this example.

## Continue from here

Try sending a question or correction relevant to real work, rather than only a
greeting. The sessions can keep their own native tools and delegation.

For managed collaborators, see [ongoing work with lanes](docs/USAGE.md#ongoing-work-with-lanes).
A new lane joins its parent's private group and its own; it does not inherit
every other group. Use `extra_groups` when an existing reviewer or collector
needs visibility. Cross-host use adds a
[federation hub](README.md#connect-hosts-to-a-hub); local use does not need one.
