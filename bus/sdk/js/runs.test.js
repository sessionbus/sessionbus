// SPDX-License-Identifier: MIT
"use strict";
const assert = require("node:assert/strict"), test = require("node:test");
const { getEventListeners } = require("node:events");
const { performance } = require("node:perf_hooks");
const { Caller, Connection, ProtocolError, Worker } = require("./index.js");
const { pair, deferred } = require("./test-support.js");
const ref = { session_id: "native@local", run_id: "g/1" }, done = { outcome: "completed", result: "retained" };
const delivery = { message_id: "original", from: { session_id: "peer@local", product: "peer", groups: [] }, body: "hello", run_id: ref.run_id };
const open = { name: "parent@label/leaf@local", groups: [], open: {} };
async function cursor(t, options = {}) {
  const [client, server] = pair(), hello = deferred(); let runs = 0, opens = 0;
  const callbacks = {
    hello: () => ({ product: "fixture", supported_open_fields: [], extra_arguments: [], supports_message_run: !!options.supportsWake }),
    open: async () => { opens++; await options.open?.(); return { session_id: "native" }; },
    run: async (...args) => { runs++; return options.run ? options.run(...args) : done; },
    interrupt: async (...args) => { await options.interrupt?.(...args); }, deliver: async (...args) => options.deliver ? options.deliver(...args) : ({ disposition: "written" }), close: async () => { await options.close?.(); },
  };
  const worker = new Worker(callbacks, { SESSIONBUS_SOCKET: "/fixture", SESSIONBUS_LAUNCH_TOKEN: "token" }, { connect: () => client });
  const handle = worker._handle.bind(worker); worker._handle = (r) => { options.observe?.(r); handle(r); };
  const bus = new Connection(server, false, (r) => {
    if (r.method === "session.hello") { void bus.result(r, {}); hello.resolve(); }
    if (r.method === "turn.ready") { if (options.ready) options.ready(r, bus); else void bus.result(r, {}); }
  });
  const serving = worker.serve().catch((error) => error);
  t.after(async () => { worker.shutdown(); bus.close(); await serving; });
  await hello.promise; if (!options.pendingOpen) await bus.call("session.open", open);
  return { worker, bus, client, runs: () => runs, opens: () => opens };
}
const execute = (bus, overrides = {}) => bus.call("turn.execute", { ...ref, input: "work", ...overrides });
const code = (value) => (error) => error instanceof ProtocolError && error.code === value;
test("ended-run delivery is refused before native submission, before and after ready ack", async (t) => {
  const ready = deferred(); let delivered = 0;
  const { bus } = await cursor(t, { ready: (request) => ready.resolve(request), deliver: () => { delivered++; return { disposition: "written" }; } });
  await execute(bus); const request = await ready.promise;
  const message = structuredClone(delivery); delete message.run_id;
  await assert.rejects(bus.call("message.deliver", message), code(-32004));
  await bus.result(request, {}); await bus.call("turn.wait", ref);
  await assert.rejects(bus.call("message.deliver", message), code(-32004));
  assert.equal(delivered, 0);
});
test("product pre-submission handoff refusal preserves NotRunning", async (t) => {
  const release = deferred();
  const { bus } = await cursor(t, { run: async () => { await release.promise; return done; }, deliver: () => { throw new ProtocolError({ code: -32004, message: "not_running" }); } });
  await execute(bus);
  const message = structuredClone(delivery); delete message.run_id;
  await assert.rejects(bus.call("message.deliver", message), code(-32004));
  release.resolve(); await bus.call("turn.wait", ref);
});
for (const action of [false, true]) for (const timeout_ms of [undefined, 60000]) for (const collect of ["status", "wait"]) {
  test(`canceled ${action ? "action" : "direct"} wait, timeout=${timeout_ms}, replacement ${collect}`, async (t) => {
    const release = deferred(), admitted = deferred();
    const { worker, bus } = await cursor(t, { run: async () => { await release.promise; return done; }, observe: (r) => { if (r.method === "turn.wait") admitted.resolve(); } });
    await execute(bus);
    const caller = new Caller(bus), cancel = new AbortController(), request = { ...ref, ...(timeout_ms === undefined ? {} : { timeout_ms }) };
    const waiting = action ? caller.action("wait", request, cancel.signal) : caller.wait(request, cancel.signal);
    await admitted.promise; cancel.abort(new Error("cancelled wait")); await assert.rejects(waiting, /cancelled wait/);
    assert.equal(getEventListeners(cancel.signal, "abort").length, 0); release.resolve();
    const replacement = new Caller(bus), expected = { ...ref, state: "done", result: done };
    assert.deepEqual(await replacement.wait(ref), expected);
    await assert.rejects(caller.wait(ref, cancel.signal), /cancelled wait/);
    assert.deepEqual(await replacement[collect](ref), expected);
    await assert.rejects(replacement.ack(ref, cancel.signal), /cancelled wait/);
    assert.deepEqual(await replacement.status(ref), expected);
    await replacement.ack(ref); await replacement.ack(ref); await assert.rejects(replacement.status(ref), code(-32001));
    assert.equal(bus.pending.size, 0); assert.equal(worker.waiters, 0); assert.equal(getEventListeners(worker.controller.signal, "abort").length, 0);
  });
}
test("canonical identity and contiguous admission bind ack", async (t) => {
  const { bus, runs } = await cursor(t);
  await assert.rejects(execute(bus, { session_id: "foreign@local" }), code(-32600));
  await assert.rejects(execute(bus, { run_id: "g/2" }), code(-32600)); assert.equal(runs(), 0);
  await execute(bus); assert.equal((await bus.call("turn.wait", ref)).state, "done");
  await bus.call("turn.ack", ref); await bus.call("turn.ack", ref);
  await assert.rejects(bus.call("turn.ack", { ...ref, run_id: "g/2" }), code(-32001));
  await execute(bus, { run_id: "g/2" }); await bus.call("turn.wait", { ...ref, run_id: "g/2" });
});
test("pending Open rejects seed before native admission", async (t) => {
  const entered = deferred(), release = deferred();
  const { bus, runs } = await cursor(t, { pendingOpen: true, open: async () => { entered.resolve(); await release.promise; } });
  const opening = bus.call("session.open", open); await entered.promise;
  await assert.rejects(bus.call("message.deliver", delivery), code(-32003)); assert.equal(runs(), 0);
  release.resolve(); await opening; await execute(bus); await bus.call("turn.wait", ref);
});
test("wake capability rejects before product Open", async (t) => {
  for (const supported of [false, true]) {
    const { bus, opens } = await cursor(t, { pendingOpen: true, supportsWake: supported });
    const opening = bus.call("session.open", { ...open, policy: { persistent: false, auto_close_ms: 0, notify: false, owner_session_id: "peer@local" } });
    if (supported) { await opening; assert.equal(opens(), 1); } else { await assert.rejects(opening, code(-32008)); assert.equal(opens(), 0); }
  }
});
test("seed preserves delivery and reports receipt separately", async (t) => {
  const { bus } = await cursor(t, { run: async (_signal, run, seed) => {
    const expected = structuredClone(delivery); delete expected.run_id; assert.deepEqual(seed, { delivery: expected });
    await run.ReportDelivery({ disposition: "written" }); await assert.rejects(run.ReportDelivery({ disposition: "written" }), /already reported/); return done;
  } });
  assert.deepEqual(await bus.call("message.deliver", delivery), { disposition: "written" });
  assert.deepEqual(await bus.call("turn.wait", ref), { ...ref, state: "done", result: done });
});
test("invalid receipt closes original call; absent receipt stays uncertain", async (t) => {
  const reported = deferred();
  const { bus, worker } = await cursor(t, { run: async (_signal, run) => {
    try { await run.ReportDelivery({ disposition: "invented" }); reported.resolve(null); } catch (error) { reported.resolve(error); } return done;
  } });
  await assert.rejects(bus.call("message.deliver", delivery)); assert.ok(await reported.promise); await worker.connection.done;
  const second = await cursor(t); await assert.rejects(second.bus.call("message.deliver", delivery), code(-32603));
  assert.equal((await second.bus.call("turn.wait", ref)).state, "done");
});
test("largest reply envelope is checked before retaining output", async (t) => {
  const status = { ...ref, state: "done", result: { outcome: "completed", result: "" } };
  const frame = (id) => `${JSON.stringify({ jsonrpc: "2.0", id, result: status })}\n`;
  const remaining = (1 << 20) - Buffer.byteLength(frame(1));
  status.result.result = "\x00".repeat(Math.floor(remaining / 6)) + "x".repeat(remaining % 6);
  assert.equal(Buffer.byteLength(frame(1)), 1 << 20); assert.ok(Buffer.byteLength(frame(Number.MAX_SAFE_INTEGER)) > 1 << 20);
  const { bus } = await cursor(t, { run: async () => status.result }); await execute(bus);
  const got = await bus.call("turn.wait", ref); assert.equal(got.state, "unavailable"); assert.equal(got.result, undefined);
});
test("FIFO cursor is bounded across replacement callers", async (t) => {
  const { worker, bus } = await cursor(t);
  for (let sequence = 1; sequence <= 256; sequence++) { const run_id = `g/${sequence}`; await execute(bus, { run_id }); await bus.call("turn.wait", { ...ref, run_id }); }
  await assert.rejects(execute(bus, { run_id: "g/257" }), code(-32003));
  await assert.rejects(bus.call("turn.ack", { ...ref, run_id: "g/2" }), code(-32003));
  assert.equal((await new Caller(bus).status({ session_id: ref.session_id })).run_id, "g/1");
  await bus.call("turn.ack", ref); await execute(bus, { run_id: "g/257" }); await bus.call("turn.wait", { ...ref, run_id: "g/257" }); assert.equal(worker.records.length, 256);
});
test("orderly close drains admitted read before product Close", async (t) => {
  const release = deferred(), admitted = deferred(), closeEntered = deferred(), closeRelease = deferred();
  const { worker, bus } = await cursor(t, { run: async () => { await release.promise; return done; }, observe: (r) => { if (r.method === "turn.wait") admitted.resolve(); }, close: async () => { closeEntered.resolve(); await closeRelease.promise; } });
  await execute(bus); const read = bus.call("turn.wait", ref); await admitted.promise;
  const closing = bus.call("session.close", { session_id: ref.session_id }); release.resolve();
  assert.equal((await read).state, "done"); await closeEntered.promise; assert.equal(worker.waiters, 0);
  await assert.rejects(bus.call("turn.status", ref), code(-32003)); closeRelease.resolve(); await closing;
});

for (const wallJump of [-10000000000, 10000000000]) test(`long explicit wait uses monotonic deadline across wall jump ${wallJump}`, { timeout: 2000 }, async (t) => {
  const release = deferred(), admitted = deferred();
  const { bus } = await cursor(t, { run: async () => { await release.promise; return done; }, observe: (r) => { if (r.method === "turn.wait") admitted.resolve(); } });
  await execute(bus);
  let elapsed = 0, wall = 1000;
  t.mock.method(performance, "now", () => elapsed); t.mock.method(Date, "now", () => wall);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let returned = false;
  const reading = bus.call("turn.wait", { ...ref, timeout_ms: 2147483667 }).then((value) => { returned = true; return value; });
  await admitted.promise;
  elapsed += 2147483647; wall += wallJump;
  t.mock.timers.tick(2147483647); await new Promise(setImmediate); assert.equal(returned, false);
  elapsed += 20; t.mock.timers.tick(20); assert.equal((await reading).state, "running");
  release.resolve(); assert.equal((await bus.call("turn.wait", ref)).state, "done");
});

test("cancelled long segmented wait drains at terminal without consuming", async (t) => {
  const release = deferred(), admitted = deferred();
  const { worker, bus } = await cursor(t, { run: async () => { await release.promise; return done; }, observe: (r) => { if (r.method === "turn.wait") admitted.resolve(); } });
  await execute(bus);
  let elapsed = 0;
  t.mock.method(performance, "now", () => elapsed); t.mock.timers.enable({ apis: ["setTimeout"] });
  const cancel = new AbortController();
  const rejected = assert.rejects(new Caller(bus).wait({ ...ref, timeout_ms: 2147483667 }, cancel.signal), /cancel segmented wait/);
  await admitted.promise; elapsed += 2147483647; t.mock.timers.tick(2147483647);
  cancel.abort(new Error("cancel segmented wait")); await rejected;
  release.resolve(); assert.equal((await new Caller(bus).wait(ref)).state, "done");
  assert.equal(bus.pending.size, 0); assert.equal(worker.waiters, 0);
  assert.equal((await new Caller(bus).status(ref)).result.result, "retained");
});

for (const closing of [false, true]) test(`native terminal disables interrupt during held fallback receipt; close=${closing}`, { timeout: 2000 }, async (t) => {
  const held = deferred(), release = deferred(), closeEntered = deferred(), interruptEntered = deferred(); let captured, interrupts = 0;
  const { worker, bus, client } = await cursor(t, {
    run: async (_signal, run) => { captured = run; return done; },
    interrupt: () => { interrupts++; },
    observe: (r) => { if (r.method === "session.close") closeEntered.resolve(); if (r.method === "turn.interrupt") interruptEntered.resolve(); },
  });
  const write = client._write.bind(client);
  client._write = (chunk, encoding, callback) => {
    if (JSON.parse(chunk).error?.code === -32603) {
      write(chunk, encoding, (error) => { held.resolve(); void release.promise.then(() => callback(error)); });
    } else write(chunk, encoding, callback);
  };
  const receipt = assert.rejects(bus.call("message.deliver", delivery), code(-32603));
  await held.promise;
  assert.equal(captured.controller.signal.aborted, true);
  let closed, interrupted;
  if (closing) {
    closed = bus.call("session.close", { session_id: ref.session_id });
    await closeEntered.promise;
    await new Promise(setImmediate);
  } else {
    interrupted = assert.rejects(bus.call("turn.interrupt", { session_id: ref.session_id }), code(-32004));
    await interruptEntered.promise; await new Promise(setImmediate);
  }
  assert.equal(interrupts, 0); assert.equal(captured.Interrupted(), false);
  release.resolve(); await receipt; await interrupted;
  if (closing) { await closed; await worker.closed; }
  else assert.equal((await bus.call("turn.wait", ref)).state, "done");
});

test("close during held ready does not interrupt an already terminal run", { timeout: 2000 }, async (t) => {
  const ready = deferred(), closing = deferred(); let captured, interrupts = 0;
  const { bus } = await cursor(t, {
    run: async (_signal, run) => { captured = run; return done; }, interrupt: () => { interrupts++; },
    ready: (request) => ready.resolve(request), observe: (r) => { if (r.method === "session.close") closing.resolve(); },
  });
  await execute(bus); const request = await ready.promise;
  const closed = bus.call("session.close", { session_id: ref.session_id });
  await closing.promise; await new Promise(setImmediate);
  assert.equal(captured.controller.signal.aborted, true); assert.equal(captured.Interrupted(), false); assert.equal(interrupts, 0);
  await bus.result(request, {}); await closed; await captured.Done;
});

for (const terminal of [false, true]) test(`close EOF settles captured Run and close routine; terminal=${terminal}`, { timeout: 2000 }, async (t) => {
  const entered = deferred(), release = deferred(), ready = deferred(), closing = deferred(), returned = deferred();
  const { worker, bus } = await cursor(t, {
    run: async (_signal, run) => { entered.resolve(run); await release.promise; return done; },
    ready: (request) => ready.resolve(request),
  });
  const close = worker._close.bind(worker);
  worker._close = async (...args) => { closing.resolve(); try { await close(...args); } finally { returned.resolve(); } };
  await execute(bus); const run = await entered.promise;
  if (terminal) { release.resolve(); await ready.promise; }
  const rejected = assert.rejects(bus.call("session.close", { session_id: ref.session_id }));
  await closing.promise; bus.close();
  await Promise.all([run.Done, returned.promise, worker.closed, rejected]);
  release.resolve();
});
