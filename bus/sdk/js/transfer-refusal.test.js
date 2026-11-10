// SPDX-License-Identifier: MIT
"use strict";
const test = require("node:test"), assert = require("node:assert/strict");
const { Connection, Transfers } = require("./index.js");
const { pair } = require("./test-support.js");

for (const state of ["missing", "disabled", "cancelled", "different-wire"]) {
  test(`transfer ${state} generation sends refusal and preserves connection`, { timeout: 2000 }, async t => {
    const [one, two] = pair(), life = new AbortController(), transfers = new Transfers({ openTransferSource: async () => { throw new Error("unexpected filesystem call"); }, openTransferDestination: async () => { throw new Error("unexpected filesystem call"); } });
    let client, server;
    client = new Connection(one, true, r => assert.equal(transfers.handleConnectionRequest(client, r), true));
    server = new Connection(two, false, r => { void server.result(r, { sessions: [] }); });
    t.after(() => { client.close(); server.close(); });
    if (state !== "missing") transfers.current = { wire: state === "different-wire" ? {} : client, enabled: state !== "disabled", controller: life };
    if (state === "cancelled") life.abort();
    await assert.rejects(server.call("transfer.prepare", { id: "x", role: "source" }), { code: -32018, message: "unsupported_transfer" });
    assert.equal(client.signal.aborted, false);
    assert.deepEqual(await client.call("session.list", {}), { sessions: [] });
  });
}
