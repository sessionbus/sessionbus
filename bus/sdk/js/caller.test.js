// SPDX-License-Identifier: MIT

"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const { ACTIONS, Caller, connectPeer, Connection, ProtocolError } = require("./index.js");
const { pair, deferred } = require("./test-support.js");

const fixtures = JSON.parse(fs.readFileSync(path.join(__dirname, "../go/protocol/caller-sugar.fixtures.json"), "utf8"));

const replies = {
  "session.list": { sessions: [] },
  "message.send": { message_id: "message", deliveries: [] },
  "lane.describe": { product: "example-peer", supported_open_fields: [], extra_arguments: [] },
  "lane.spawn": { session_id: "lane@local" },
  "trace.configure": { session_id: "lane@local", mode: "events" },
  "turn.run": { session_id: "lane@local", run_id: "g/1", state: "done", result: { outcome: "completed", result: "done" } },
  "turn.interrupt": {},
  "session.close": {},
};

test("caller maps operations to closed wire requests", async (t) => {
  const [clientSocket, daemonSocket] = pair();
  const seen = [];
  const daemon = new Connection(daemonSocket, false, (request) => { seen.push([request.method, request.params]); void daemon.result(request, replies[request.method]); });
  const connection = new Connection(clientSocket, true);
  const caller = new Caller(connection);
  t.after(() => { connection.close(); daemon.close(); });

  const table = [
    [() => caller.list({}), "session.list", {}],
    [() => caller.send({ target: "lane", message: "hello" }), "message.send", { target: "lane", message: "hello" }],
    [() => caller.describe({ product: "example-peer" }), "lane.describe", { product: "example-peer" }],
    [() => caller.spawn({ name: "child", product: "example-peer", open: {}, trace: "content" }), "lane.spawn", { name: "child", product: "example-peer", open: {}, trace: "content" }],
    [() => caller.resume("lane@local"), "lane.spawn", { resume_session_id: "lane@local" }],
    [() => caller.trace({ session_id: "lane@local", mode: "events" }), "trace.configure", { session_id: "lane@local", mode: "events" }],
    [() => caller.run({ session_id: "lane@local", input: "work" }), "turn.run", { session_id: "lane@local", input: "work" }],
    [() => caller.interrupt({ session_id: "lane@local" }), "turn.interrupt", { session_id: "lane@local" }],
    [() => caller.close({ session_id: "lane@local", forget: true }), "session.close", { session_id: "lane@local", forget: true }],
  ];
  for (const [call, method, params] of table) { await call(); assert.deepEqual(seen.shift(), [method, params]); }

  const actions = [
    ["list", {}, "session.list", {}],
    ["send", { target: "lane", message: "hello" }, "message.send", { target: "lane", message: "hello" }],
    ["spawn", { name: "child", product: "example-peer", open: {}, trace: "events" }, "lane.spawn", { name: "child", product: "example-peer", open: {}, trace: "events" }],
    ["trace", { session_id: "lane@local", mode: "content" }, "trace.configure", { session_id: "lane@local", mode: "content" }],
    ["describe", { product: "example-peer" }, "lane.describe", { product: "example-peer" }],
    ["run", { session_id: "lane@local", input: "work" }, "turn.run", { session_id: "lane@local", input: "work" }],
    ["interrupt", { session_id: "lane@local" }, "turn.interrupt", { session_id: "lane@local" }],
    ["close", { session_id: "lane@local" }, "session.close", { session_id: "lane@local" }],
    ["forget", { session_id: "lane@local" }, "session.close", { session_id: "lane@local", forget: true }],
  ];
  for (const [action, args, method, params] of actions) { await caller.action(action, args); assert.deepEqual(seen.shift(), [method, params]); }
  await assert.rejects(caller.spawn({ resume_session_id: "lane@local", name: "child" }), (error) => error.message === `LaneSpawnRequest: "name" is not allowed with "resume_session_id"`);
  assert.equal(seen.length, 0);
  assert.deepEqual(ACTIONS, ["list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack", "transfer.offer", "transfer.request", "transfer.accept", "transfer.cancel", "transfer.status"]);
  await assert.rejects(caller.action("unknown", {}), /unknown action/);
  await assert.rejects(caller.action("list", { extra: true }), /SessionListRequest: "extra" is not allowed/);
  await assert.rejects(caller.action("trace", { session_id: "lane@local", mode: "all" }), /TraceConfigureRequest: "mode" must be one of/);
  await assert.rejects(caller.action("status", { turn_id: "missing", extra: true }), /ReadRequest:/);
});

test("caller operations use shared wire fixtures", async (t) => {
 const [client, server] = pair(); let current;
 const daemon = new Connection(server, false, (request) => { assert.equal(request.method, current.method); assert.deepEqual(request.params, current.request); void daemon.result(request, current.result); });
 const connection = new Connection(client, true), caller = new Caller(connection);
 t.after(() => { connection.close(); daemon.close(); });
 for (const row of fixtures.operations) { current = row; assert.deepEqual(await caller.action(row.action, row.request), row.result); }
});

test("submitted ack settles after cancellation; pre-aborted ack sends nothing", async (t) => {
 const [client, server] = pair(), received = deferred(); let writes = 0;
 const daemon = new Connection(server, false, (request) => { writes++; received.resolve(request); });
 const connection = new Connection(client,true), caller = new Caller(connection);
 t.after(() => { connection.close(); daemon.close(); });
 const ref = { session_id: "lane@local", run_id: "g/1" }, cancel = new AbortController();
 const ack = caller.action("ack",ref,cancel.signal), request = await received.promise;
 cancel.abort(new Error("cancelled")); await daemon.result(request,{}); assert.deepEqual(await ack,{});
 await assert.rejects(caller.ack(ref,cancel.signal),/cancelled/); assert.equal(writes,1);
});

test("crossed rehello preserves the fixture's newest identity", async (t) => {
  const [clientSocket, daemonSocket] = pair();
  const queued = [], waiting = [];
  const next = () => queued.length ? Promise.resolve(queued.shift()) : new Promise((resolve) => waiting.push(resolve));
  const daemon = new Connection(daemonSocket, false, (request) => { const resolve = waiting.shift(); if (resolve) resolve(request); else queued.push(request); });
  const row = structuredClone(fixtures.sequences.crossed_rehello);
  const env = { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" };
  const peer = connectPeer(row.identity, async () => ({ disposition: "injected" }), env, { connect: () => clientSocket, schedule: () => {} });
  t.after(() => { peer.shutdown(); daemon.close(); });

  row.identity.info.nested.value = "mutated";
  const initial = await next();
  assert.deepEqual([initial.params.name, initial.params.info.nested.value], ["initial", "initial"]);
  await daemon.result(initial, {}); await peer.ready;

  const first = peer.rehello(undefined, row.first.name, row.first.info); const firstRequest = await next(); row.first.info.nested.value = "mutated";
  const second = peer.rehello(undefined, row.second.name, row.second.info); const secondRequest = await next(); row.second.info.nested.value = "mutated";
  await daemon.result(secondRequest, {}); await second;
  await daemon.result(firstRequest, {});
  const corrective = await next();
  assert.deepEqual([corrective.params.name, corrective.params.info.nested.value], ["second", "second"]);
  await daemon.result(corrective, {}); await first;
  assert.deepEqual([peer.identity.name, peer.identity.info.nested.value], ["second", "second"]);
});
