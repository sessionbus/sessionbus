// SPDX-License-Identifier: MIT

"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const { EventEmitter } = require("node:events");
const { connectPeer, Connection, ProtocolError, Worker } = require("./index.js");
const { pair, deferred } = require("./test-support.js");

const rows = JSON.parse(fs.readFileSync(path.join(__dirname, "../go/protocol/session-lifecycle.fixtures.json"), "utf8"));
const openRequest = { name: "parent/leaf@local", groups: ["session:parent"], resume_session_id: "product-session", open: { cwd: "/tmp", permission_mode: "ask", model: "model", reasoning_effort: "high", arguments: ["--flag"] } };
const target = { session_id: "product-session@local" };
const delivery = { message_id: "m", from: { session_id: "peer@local", name: "peer@local", product: "peer", groups: [] }, body: "body" };

function aborted(signal) { if (signal.aborted) return Promise.resolve(); return new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true })); }
function errorCode(promise, code) { return assert.rejects(promise, (error) => error instanceof ProtocolError && error.code === code); }

class FakeProduct {
  constructor() { this.calls = [0, 0, 0, 0, 0, 0]; }
  hello(_cancel) {
    this.calls[0]++; assert.equal(this.env.SESSIONBUS_LAUNCH_TOKEN, undefined); assert.equal(this.env.SESSIONBUS_LOCAL_KEY, undefined); assert.equal(this.env.SESSIONBUS_SOCKET, undefined);
    return { product: "example-peer", supported_open_fields: [], extra_arguments: [] };
  }
  open(_cancel, request) { this.calls[1]++; assert.deepEqual(request, openRequest); return { session_id: "product-session" }; }
  async run(cancel, run, seed) {
    const input = seed.text;
    this.calls[2]++;
    if (input === "admission") { assert.equal(this.admitted, true); this.started.resolve(); return { outcome: "completed", result: "" }; }
    if (input === "block") { this.started.resolve(run); await this.release.promise; if (run.Interrupted()) return { outcome: "interrupted", result: "" }; run.Native = "native"; return { outcome: "completed", result: "" }; }
    if (input === "eof") { this.started.resolve(run); await aborted(cancel); throw cancel.reason; }
    if (input === "admit") { this.nativeEvents.push("input"); this.started.resolve(run); await this.admit.promise; run.Admitted(); run.Admitted(); await this.release.promise; return { outcome: "completed", result: "" }; }
    if (input === "fail") return Promise.reject(new Error("failed exactly"));
    if (input === "long") return { outcome: "completed", result: "x".repeat(262145) };
    return { outcome: input === "interrupted" ? "interrupted" : "completed", result: input === "completed" || input === "interrupted" ? "" : input };
  }
  async interrupt(cancel, run) { this.calls[3]++; assert.equal(run.Interrupted(), true); this.interrupted?.resolve(); if (this.hangInterrupt) await aborted(cancel); if (this.interruptError) throw this.interruptError; }
  async deliver(cancel, _request, _identity, run) {
    this.calls[4]++; if (this.deliverStart) { this.deliverStart.resolve(); await aborted(cancel); throw cancel.reason; }
    if (this.deliverRun) {
      this.deliverRun.resolve(run);
      if (this.deliverRelease) await this.deliverRelease.promise;
      if (!run) throw new ProtocolError({ code: -32004, message: "not_running" });
      const state = await Promise.race([run.Done.then(() => "done"), run.AdmittedDone.then(() => "admitted")]);
      this.admissionSelected?.resolve(); if (this.afterAdmission) await this.afterAdmission.promise;
      if (state === "done" || run.finished) throw new ProtocolError({ code: -32004, message: "not_running" });
      this.nativeEvents?.push("steer"); this.steered?.resolve(); return { disposition: "injected" };
    }
    if (this.outbound) await this.worker.caller.list({}); return { disposition: "injected" };
  }
  async close(cancel, request) { this.calls[5]++; this.closeSignal = cancel; this.closeRequest = request; this.closeAborted = cancel.aborted; this.closeStart?.resolve(); if (this.closeEnd) await this.closeEnd.promise; if (this.closeError) throw this.closeError; }
}

test("worker idle delivery requires a seed", async (t) => {
  const product = new FakeProduct(); const { daemon } = await harness(t, product);
  await errorCode(daemon.call("message.deliver", delivery), -32004); assert.equal(product.calls[4], 0);
});

test("worker delivery keeps its admission run across terminal", async (t) => {
  const product = new FakeProduct(); product.started = deferred(); product.release = deferred(); product.deliverRun = deferred(); product.deliverRelease = deferred(); const { daemon } = await harness(t, product);
  const running = daemon.call("turn.run", { ...target, input: "block" }); const run = await product.started.promise, delivered = daemon.call("message.deliver", delivery); assert.equal(await product.deliverRun.promise, run);
  product.release.resolve(); await running; await run.Done; let admitted = false; run.AdmittedDone.then(() => { admitted = true; }); await Promise.resolve(); assert.equal(admitted, false);
  product.deliverRelease.resolve(); await errorCode(delivered, -32004);
});

test("run admission orders active delivery", async (t) => {
  const product = new FakeProduct(); product.started = deferred(); product.release = deferred(); product.admit = deferred(); product.deliverRun = deferred(); product.nativeEvents = []; product.steered = deferred(); const { daemon } = await harness(t, product);
  const running = daemon.call("turn.run", { ...target, input: "admit" }); const run = await product.started.promise, delivered = daemon.call("message.deliver", delivery); assert.equal(await product.deliverRun.promise, run); assert.deepEqual(product.nativeEvents, ["input"]);
  product.admit.resolve(); await product.steered.promise; assert.deepEqual(product.nativeEvents, ["input", "steer"]); assert.equal((await delivered).disposition, "injected"); product.release.resolve(); await running;
});

test("finished run wins after admission is selected", async (t) => {
  const product = new FakeProduct(); product.started = deferred(); product.release = deferred(); product.admit = deferred(); product.deliverRun = deferred(); product.nativeEvents = []; product.admissionSelected = deferred(); product.afterAdmission = deferred(); const { daemon } = await harness(t, product);
  const running = daemon.call("turn.run", { ...target, input: "admit" }); const run = await product.started.promise, delivered = daemon.call("message.deliver", delivery); assert.equal(await product.deliverRun.promise, run);
  product.admit.resolve(); await product.admissionSelected.promise; product.release.resolve(); await running; await run.Done; product.afterAdmission.resolve(); await errorCode(delivered, -32004); assert.deepEqual(product.nativeEvents, ["input"]);
});

test("connection write failure rejects once", async (t) => {
  const [clientSocket, daemonSocket] = pair(); clientSocket.failNext = true; const connection = new Connection(clientSocket, true);
  t.after(() => { connection.close(); daemonSocket.destroy(); });
  await assert.rejects(connection.call("session.list", {}), /write failed/); await Promise.resolve(); assert.equal(connection.pending.size, 0);
});

test("pre-aborted call has no pending request or write", async (t) => {
  const [clientSocket, daemonSocket] = pair(); const connection = new Connection(clientSocket, true); let seen = 0; daemonSocket.on("data", (body) => { seen += body.length; });
  t.after(() => { connection.close(); daemonSocket.destroy(); });
  const cancel = new AbortController(); cancel.abort(new Error("already cancelled")); await assert.rejects(connection.call("session.list", {}, cancel.signal), /already cancelled/);
  assert.equal(seen, 0); assert.equal(connection.pending.size, 0);
});

test("unmatched response closes the connection", async () => {
  const [clientSocket, daemonSocket] = pair(), connection = new Connection(clientSocket, true);
  daemonSocket.write('{"jsonrpc":"2.0","id":99,"result":{}}\n'); await connection.done;
  await assert.rejects(connection.call("session.list", {}), /closed/);
});

test("worker closed resolves when product close rejects", async (t) => {
  const product = new FakeProduct(); product.closeError = new Error("close failed"); const stderr = captureStderr(t); const { worker, daemon, serving } = await harness(t, product); daemon.close();
  await worker.closed; await serving; assert.equal(product.calls[5], 1); assert.equal(stderr(), 'sessionbus: product close: "close failed"\n');
});

test("worker waits for orderly product close across EOF", async (t) => {
  const product = new FakeProduct(); product.started = deferred(); product.release = deferred(); product.interrupted = deferred(); product.closeStart = deferred(); product.closeEnd = deferred(); const { worker, daemon, serving } = await harness(t, product); let closed = false; worker.closed.then(() => { closed = true; });
  const running = daemon.call("turn.run", { ...target, input: "block" }).catch((error) => error); await product.started.promise; const request = { ...target, forget: true }, closing = daemon.call("session.close", request).catch((error) => error); await product.interrupted.promise; daemon.close(); await product.closeStart.promise;
  assert.equal(product.closeSignal.aborted, true); assert.deepEqual(product.closeRequest, request); assert.equal(closed, false); product.release.resolve(); product.closeEnd.resolve(); await worker.closed; await serving; await Promise.all([running, closing]); assert.deepEqual(product.closeRequest, request); assert.equal(product.calls[5], 1);
});

test("one chunk is admitted before its first callback", async (t) => {
  const product = new FakeProduct(); product.started = deferred(); const { worker } = await harness(t, product);
  worker.connection.result = () => Promise.resolve(); worker.connection.error = () => { product.admitted = true; return Promise.resolve(); };
  const frame = (id, input) => JSON.stringify({ jsonrpc: "2.0", id, method: "turn.execute", params: { ...target, run_id: `fixture/${id-1}`, input } }) + "\n";
  worker.connection._data(Buffer.from(frame(2, "admission") + frame(3, "again"))); await product.started.promise;
});

test("peer replace settles old runs and reconnects with the new identity", async (t) => {
  const first = peerEndpoint(), second = peerEndpoint(), clients = [first.client, second.client], scheduled = [], scheduledReady = deferred();
  const identity = { product: "native-product", session_id: "old", name: "old", groups: ["old"], info: {} };
  const peer = connectPeer(identity, async () => ({ disposition: "injected" }), { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => clients.shift(), schedule: (call) => { scheduled.push(call); scheduledReady.resolve(); } });
  t.after(() => { peer.shutdown(); first.daemon.close(); second.daemon.close(); });
  await first.daemon.result(await first.next(), {}); await peer.ready;
  const before = structuredClone(peer.identity);
  await assert.rejects(peer.replace({ ...before, name: "same" }), /invalid replace identity/); assert.deepEqual(peer.identity, before);
  const starting = peer.caller.start({ session_id: "lane@local", input: "block" }), run = await first.next();
  await first.daemon.result(run, { session_id: "lane@local", run_id: "g/1" }); const started = await starting;
  const replacementIdentity = { product: "native-product", session_id: "new", name: "new", groups: ["new"], info: { revision: 1 } };
  const replaced = peer.replace(replacementIdentity), replacement = await first.next(); replacementIdentity.groups.push("mutated"); replacementIdentity.info.revision = 9;
  await assert.rejects(peer.caller.wait(started), /not connected/);
  await assert.rejects(peer.call("session.list", {}), /not connected/);
  const update = { name: "retitled", info: { revision: 2 } }; await assert.rejects(peer.rehello(undefined, update.name, update.info), /not connected/); update.info.revision = 9;
  await first.daemon.result(replacement, {});
  const corrective = await first.next(); assert.deepEqual(corrective.params, { protocol: 1, product: "native-product", session_id: "new", name: "retitled", groups: ["new"], info: { revision: 2 } });
  await first.daemon.result(corrective, {}); await replaced;
  first.daemon.close(); await scheduledReady.promise; scheduled.shift()();
  const reconnect = await second.next(); assert.deepEqual(reconnect.params, corrective.params); await second.daemon.result(reconnect, {}); await peer.ready;
});

test("peer delivery keeps its admission identity across replace", async (t) => {
  const endpoint = peerEndpoint(), entered = deferred();
  const identity = { product: "native-product", session_id: "old", name: "old", groups: ["old"], info: { nested: { value: "old" } } };
  const peer = connectPeer(identity, async (signal, _request, admitted) => {
    const seen = structuredClone(admitted); admitted.groups[0] = "mutated"; admitted.info.nested.value = "mutated"; entered.resolve({ seen, signal }); await aborted(signal);
    return { disposition: "rejected", reason: "closing" };
  }, { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => { peer.shutdown(); endpoint.daemon.close(); });
  await endpoint.daemon.result(await endpoint.next(), {}); await peer.ready;
  const delivering = endpoint.daemon.call("message.deliver", delivery), admission = await entered.promise;
  assert.deepEqual(admission.seen, identity); assert.deepEqual(peer.identity, identity);
  const replacing = peer.replace({ product: "native-product", session_id: "new", name: "new", groups: ["new"], info: {} }), replacement = await endpoint.next();
  assert.equal(admission.signal.aborted, true); assert.deepEqual(await delivering, { disposition: "rejected", reason: "closing" });
  await endpoint.daemon.result(replacement, {}); await replacing;
});

test("peer installs delivery identity before reading the next frame", async (t) => {
  const endpoint = peerEndpoint(), called = deferred();
  const identity = { product: "native-product", session_id: "session", name: "peer", groups: [], info: {} };
  const peer = connectPeer(identity, async (signal, _request, admitted) => { called.resolve({ identity: admitted, signal }); return { disposition: "injected" }; }, { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => { peer.shutdown(); endpoint.daemon.close(); });
  const hello = await endpoint.next(), response = { jsonrpc: "2.0", id: hello.id, result: {} }, request = { jsonrpc: "2.0", id: 1, method: "message.deliver", params: delivery };
  endpoint.daemon.stream.write(`${JSON.stringify(response)}\n${JSON.stringify(request)}\n`); await peer.ready;
  const admission = await called.promise; assert.equal(admission.identity.session_id, "session"); assert.equal(admission.signal.aborted, false);
});

test("peer delivery context aborts on EOF", async (t) => {
  const endpoint = peerEndpoint(), entered = deferred(), finished = deferred();
  const peer = connectPeer({ product: "native-product", session_id: "session", name: "peer", groups: [], info: {} }, async (signal) => { entered.resolve(signal); await aborted(signal); finished.resolve(); return { disposition: "rejected", reason: "closing" }; }, { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => peer.shutdown()); await endpoint.daemon.result(await endpoint.next(), {}); await peer.ready;
  void endpoint.daemon.call("message.deliver", delivery).catch(() => {}); const signal = await entered.promise; endpoint.daemon.close(); await finished.promise; assert.equal(signal.aborted, true);
});

test("peer rehello cancellation keeps its late acknowledgement owned", async (t) => {
  const endpoint = peerEndpoint(), admitted = [];
  const peer = connectPeer({ product: "native-product", session_id: "session", name: "old", groups: [], info: {} }, async (_signal, _request, identity) => { admitted.push(identity); return { disposition: "injected" }; }, { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => { peer.shutdown(); endpoint.daemon.close(); }); await endpoint.daemon.result(await endpoint.next(), {}); await peer.ready;
  await peer.rehello(undefined, "old", {}); const called = peer.call("session.list", {}), request = await endpoint.next(); assert.equal(request.method, "session.list"); await endpoint.daemon.result(request, { sessions: [], hosts: [] }); await called;
  const controller = new AbortController(), changed = peer.rehello(controller.signal, "renamed", { revision: 2 }), hello = await endpoint.next(); controller.abort(new Error("caller cancelled"));
  assert.deepEqual(await endpoint.daemon.call("message.deliver", { ...delivery, message_id: "before" }), { disposition: "injected" }); assert.equal(admitted.shift().name, "old");
  await assert.rejects(changed, /caller cancelled/); await endpoint.daemon.result(hello, {});
  assert.deepEqual(await endpoint.daemon.call("message.deliver", { ...delivery, message_id: "after" }), { disposition: "injected" }); assert.deepEqual(admitted.shift(), { product: "native-product", session_id: "session", name: "renamed", groups: [], info: { revision: 2 } });
});

test("peer shutdown closes a held replacement hello", async (t) => {
  const endpoint = peerEndpoint(), peer = connectPeer({ product: "native-product", session_id: "old", name: "old", groups: [], info: {} }, async () => ({ disposition: "injected" }), { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => endpoint.daemon.close()); await endpoint.daemon.result(await endpoint.next(), {}); await peer.ready;
  const replacing = peer.replace({ product: "native-product", session_id: "new", name: "new", groups: [], info: {} }); await endpoint.next(); peer.shutdown(); await assert.rejects(replacing, /not connected|closed/); await peer.closed; assert.equal(peer.error, null);
});

test("peer supersession ends without waiting for an end-only socket's reply write", { timeout: 2000 }, async (t) => {
  const stream = new EventEmitter(), replies = [], scheduled = [], unhandled = [];
  // Bun's supersession failure emits only end while the reply callback stays
  // pending. Even an explicit destroy need not synchronously emit close.
  stream.write = (body, callback) => {
    const frame = JSON.parse(body);
    if (Object.hasOwn(frame, "result")) replies.push(callback);
    else callback();
    return true;
  };
  stream.destroy = () => { stream.destroyed = true; };
  const onUnhandled = (error) => unhandled.push(error);
  process.on("unhandledRejection", onUnhandled);
  const peer = connectPeer({ product: "native-product", session_id: "session", groups: [], info: {} }, async () => ({ disposition: "written" }), { SESSIONBUS_SOCKET: "/fixture/socket" }, { connect: () => stream, schedule: (call) => scheduled.push(call) });
  t.after(() => {
    for (const reply of replies) reply();
    peer.shutdown(); stream.emit("close");
    process.off("unhandledRejection", onUnhandled);
  });
  stream.emit("data", Buffer.from('{"jsonrpc":"2.0","id":1,"result":{}}\n'));
  await peer.ready;
  const identitySignal = peer.identityController.signal;
  const pending = peer.call("session.list", {}).catch((error) => error);
  let closed = false;
  peer.closed.then(() => { closed = true; });
  stream.emit("data", Buffer.from('{"jsonrpc":"2.0","id":1,"method":"session.superseded","params":{}}\n'));
  stream.emit("end");
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(replies.length, 1, "the courtesy reply was attempted but not completed");
  assert.equal(closed, true, "terminal cleanup must not wait for the reply callback or close event");
  assert.equal(peer.terminal, true);
  assert.equal(peer.error instanceof ProtocolError, true);
  assert.equal(peer.error.code, -32012);
  assert.equal(identitySignal.aborted, true);
  assert.equal(identitySignal.reason, peer.error);
  assert.equal(stream.destroyed, true);
  assert.match((await pending).message, /closed/);
  await assert.rejects(peer.call("session.list", {}), /not connected/);
  assert.equal(scheduled.length, 0);
  // Node's counterpart is an EPIPE callback. Its rejected courtesy reply is
  // observed too; neither runtime may leave an unhandled rejection.
  replies[0](new Error("write EPIPE"));
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(unhandled, []);
});

test("peer rejected hello is terminal", async (t) => {
  const endpoint = peerEndpoint(), scheduled = [];
  const peer = connectPeer({ product: "native-product", session_id: "session", name: "sentence title", groups: [], info: {} }, async () => ({ disposition: "injected" }), { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: (call) => scheduled.push(call) });
  t.after(() => endpoint.daemon.close());
  const hello = await endpoint.next(); assert.equal(hello.params.name, "sentence title"); await endpoint.daemon.error(hello, -32602); await peer.closed;
  assert.equal(peer.error instanceof ProtocolError, true); assert.equal(peer.error.code, -32602); assert.equal(scheduled.length, 0);
  let ready = false; peer.ready.then(() => { ready = true; }); await Promise.resolve(); assert.equal(ready, false);
});

for (const replacement of [false, true]) test(`peer rejected ${replacement ? "replace" : "live rehello"} is terminal`, async (t) => {
  const endpoint = peerEndpoint(), scheduled = [];
  const peer = connectPeer({ product: "native-product", session_id: "session", name: "old title", groups: [], info: {} }, async () => ({ disposition: "injected" }), { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }, { connect: () => endpoint.client, schedule: (call) => scheduled.push(call) });
  t.after(() => endpoint.daemon.close()); const hello = await endpoint.next(); await endpoint.daemon.result(hello, {}); await peer.ready;
  const changed = replacement ? peer.replace({ product: "native-product", session_id: "next", name: "next title", groups: [], info: {} }) : peer.rehello(undefined, "next title", {});
  const rejected = await endpoint.next(); await endpoint.daemon.error(rejected, -32602); await assert.rejects(changed, (error) => error instanceof ProtocolError && error.code === -32602); await peer.closed;
  assert.equal(peer.error.code, -32602); assert.equal(scheduled.length, 0); await assert.rejects(peer.call("session.list", {}), /not connected/);
});

async function harness(t, product, options = {}) {
  const [workerSocket, daemonSocket] = pair(); const hello = deferred(); const env = { SESSIONBUS_LAUNCH_TOKEN: "token", SESSIONBUS_LOCAL_KEY: "", SESSIONBUS_SOCKET: "/fixture/socket" }; product.env = env;
  const worker = new Worker(product, env, { connect: (socket) => { assert.equal(socket, "/fixture/socket"); return workerSocket; } }); product.worker = worker;
  const daemon = new Connection(daemonSocket, false, (request) => {
    if (request.method === "session.hello") { if (options.acknowledge === false) daemon.close(); else void daemon.result(request, {}); hello.resolve(); }
    if (request.method === "turn.ready") void daemon.result(request, {});
    if (request.method === "session.list") void (worker.opened ? daemon.result(request, { sessions: [] }) : daemon.error(request, -32011));
  });
  const serving = worker.serve().catch((error) => error); await hello.promise;
  t.after(async () => { worker.shutdown(); daemon.close(); await serving; });
  if (options.open !== false && options.acknowledge !== false) assert.deepEqual(await daemon.call("session.open", openRequest), { session_id: "product-session" });
  // Translate the public blocking run into the real daemon/worker protocol.
  const call = daemon.call.bind(daemon); let sequence = 0;
  daemon.call = async (method,params,signal) => {
    if (method !== "turn.run") return call(method,params,signal);
    const run_id = `fixture/${++sequence}`;
    const execute = call("turn.execute",{...params,run_id},signal);
    const reading = call("turn.wait",{session_id:params.session_id,run_id},signal); reading.catch(() => {});
    try { await execute; } catch (error) { sequence--; throw error; }
    return reading;
  };
  return { worker, daemon, workerSocket, serving };
}

function peerEndpoint() {
  const [client, socket] = pair(), queued = [], waiting = [];
  const daemon = new Connection(socket, false, (request) => { const resolve = waiting.shift(); if (resolve) resolve(request); else queued.push(request); });
  return { client, daemon, next: () => queued.length ? Promise.resolve(queued.shift()) : new Promise((resolve) => waiting.push(resolve)) };
}

for (const row of rows) test(`lifecycle: ${row.name}`, async (t) => {
  const product = new FakeProduct(); let result;
  switch (row.name) {
    case "ready-open-commit": {
      const { worker, daemon } = await harness(t, product, { open: false }); await errorCode(worker.call("session.list", {}), -32011); await daemon.call("session.open", openRequest); await worker.call("session.list", {}); break;
    }
    case "describe-eof": { const { serving } = await harness(t, product, { acknowledge: false, open: false }); await serving; break; }
    case "terminal-results": {
      const { daemon } = await harness(t, product);
      for (const input of ["ok", "interrupted", "fail", "completed", "long"]) {
        result = await daemon.call("turn.run", { ...target, input });
        if (["fail", "long"].includes(input)) { assert.equal(result.state, "unavailable"); assert.equal(result.result, undefined); }
        else assert.deepEqual(result.result, { outcome: input === "interrupted" ? "interrupted" : "completed", result: input === "ok" ? input : "" });
      } break;
    }
    case "one-run": case "one-interrupt": case "interrupt-error": case "full-duplex": case "callback-originated-method": case "close-during-run": case "run-done": {
      product.started = deferred(); product.release = deferred(); const { daemon } = await harness(t, product); const running = daemon.call("turn.run", { ...target, input: "block" }); const run = await product.started.promise;
      if (row.name === "one-run") await errorCode(daemon.call("turn.run", { ...target, input: "again" }), -32003);
      if (row.name === "one-interrupt") { product.interrupted = deferred(); await Promise.all([daemon.call("turn.interrupt", target), daemon.call("turn.interrupt", target)]); await product.interrupted.promise; }
      if (row.name === "interrupt-error") { product.interruptError = new Error("first failure\nsecond failure"); const stderr = captureStderr(t); await daemon.call("turn.interrupt", target); assert.equal(stderr(), 'sessionbus: product interrupt: "first failure\\nsecond failure"\n'); }
      if (row.name === "full-duplex" || row.name === "callback-originated-method") { product.outbound = true; assert.equal((await daemon.call("message.deliver", delivery)).disposition, "injected"); }
      if (row.name === "run-done") { let done = false; run.Done.then(() => { done = true; }); await Promise.resolve(); assert.equal(done, false); }
      if (row.name === "close-during-run") {
        const stderr = captureStderr(t); product.interrupted = deferred(); product.closeStart = deferred(); product.closeEnd = deferred(); product.hangInterrupt = true; product.interruptError = new Error("first failure\nsecond failure"); const closing = daemon.call("session.close", target); await product.interrupted.promise; await daemon.call("turn.interrupt", target); product.release.resolve(); await running; await product.closeStart.promise; product.closeEnd.resolve(); await closing; assert.equal(stderr(), 'sessionbus: product interrupt: "first failure\\nsecond failure"\n'); break;
      }
      product.release.resolve(); result = await running; await run.Done; if (row.name === "one-interrupt") assert.equal(run.Native, null); break;
    }
    case "eof-during-run": {
      product.closeError = new Error("first failure\nsecond failure"); const stderr = captureStderr(t); product.started = deferred(); const { daemon, worker, serving } = await harness(t, product); const running = daemon.call("turn.run", { ...target, input: "eof" }); const run = await product.started.promise; daemon.close(); await serving; await assert.rejects(running); await run.Done; assert.equal(worker.controller.signal.aborted, true); assert.equal(product.closeAborted, true); assert.deepEqual(product.closeRequest, {}); assert.equal(stderr(), 'sessionbus: product close: "first failure\\nsecond failure"\n'); break;
    }
    case "peer-lifetime": { await peerLifetime(); const { daemon, serving } = await harness(t, product, { open: false }); await daemon.call("session.superseded", {}); await serving; break; }
    case "wrong-direction-request": { const { daemon, serving } = await harness(t, product, { open: false }); await errorCode(daemon.call("session.hello", { protocol: 1, product: "example-peer", launch_token: "again", supported_open_fields: [], extra_arguments: [] }), -32602); await serving; break; }
    case "terminal-before-interrupt": {
      const { daemon, worker } = await harness(t, product); const entered = deferred(), release = deferred(), call = worker.connection.call.bind(worker.connection); worker.connection.call = async (method, ...args) => { if (method === "turn.ready") { entered.resolve(); await release.promise; } return call(method, ...args); };
      const running = daemon.call("turn.run", { ...target, input: "fail" }); await entered.promise; await errorCode(daemon.call("turn.interrupt", target), -32004); release.resolve(); await running; break;
    }
    case "idle-close-deliver": {
      product.closeStart = deferred(); product.closeEnd = deferred(); const { daemon } = await harness(t, product);
      await errorCode(daemon.call("message.deliver", delivery), -32004);
      const closing = daemon.call("session.close", target); await product.closeStart.promise;
      assert.equal((await daemon.call("message.deliver", delivery)).reason, "closing"); product.closeEnd.resolve(); await closing; break;
    }
    case "environment": {
      const reads = {}, env = new Proxy({ SESSIONBUS_LAUNCH_TOKEN: "token", SESSIONBUS_LOCAL_KEY: "key", SESSIONBUS_SOCKET: "/fixture/socket" }, { get(object, name) { reads[name] = (reads[name] || 0) + 1; return object[name]; } }); product.env = env;
      const worker = new Worker(product, env); await assert.rejects(worker.serve(), /local key/); assert.deepEqual(reads, { SESSIONBUS_LAUNCH_TOKEN: 1, SESSIONBUS_LOCAL_KEY: 1, SESSIONBUS_SOCKET: 1 }); assert.deepEqual(env, {}); break;
    }
    case "shutdown": { const { worker, serving } = await harness(t, product, { open: false }); worker.shutdown(); worker.shutdown(); await serving; break; }
    case "run-done-write-failure": {
      product.started = deferred(); product.release = deferred(); const { daemon, workerSocket, serving } = await harness(t, product); const running = daemon.call("turn.run", { ...target, input: "block" }); const run = await product.started.promise; workerSocket.failNext = true; product.release.resolve(); await serving; await assert.rejects(running); await run.Done; break;
    }
    case "close-error": {
      product.closeError = new Error("first failure\nsecond failure"); const stderr = captureStderr(t); const { daemon } = await harness(t, product); assert.deepEqual(await daemon.call("session.close", target), {}); assert.equal(product.closeAborted, false); assert.deepEqual(product.closeRequest, target); assert.equal(stderr(), 'sessionbus: product close: "first failure\\nsecond failure"\n'); break;
    }
    case "close-forget": {
      product.started = deferred(); product.release = deferred(); product.interrupted = deferred(); const { worker, daemon, serving } = await harness(t, product); const running = daemon.call("turn.run", { ...target, input: "block" }).catch((error) => error); await product.started.promise;
      const request = { ...target, forget: true }, closing = daemon.call("session.close", request).catch((error) => error); await product.interrupted.promise; daemon.close(); await worker.closed; assert.deepEqual(product.closeRequest, request); assert.equal(product.closeAborted, true); product.release.resolve(); await Promise.all([serving, running, closing]); break;
    }
    default: assert.fail(`unknown row ${row.name}`);
  }
  assert.deepEqual(product.calls, row.calls);
});

function captureStderr(t) {
  const write = process.stderr.write; let output = ""; process.stderr.write = (chunk) => { output += chunk; return true; }; t.after(() => { process.stderr.write = write; }); return () => output;
}

async function peerLifetime() {
  const connections = [], scheduled = []; let scheduledReady = deferred(), holdHello; const env = { SESSIONBUS_SOCKET: "/fixture/socket", SESSIONBUS_LOCAL_KEY: "" }; let currentIdentity, deliveries = 0;
  const identity = { product: "native-product", session_id: "session-1", name: "peer", groups: ["shared"], info: {} };
  const peer = connectPeer(identity, async () => { deliveries++; return { disposition: "injected" }; }, env, { connect: () => { const [client, server] = pair(); const daemon = new Connection(server, false, (request) => {
    if (request.method === "session.hello") { if (holdHello) holdHello.resolve(); else if (currentIdentity?.session_id === request.params.session_id && JSON.stringify(currentIdentity.groups) !== JSON.stringify(request.params.groups)) void daemon.error(request, -32602); else { currentIdentity = request.params; void daemon.result(request, {}); } }
  }); connections.push(daemon); return client; }, schedule: (call, milliseconds) => { scheduled.push({ call, milliseconds }); scheduledReady.resolve(); } });
  await peer.ready; assert.deepEqual(env, {}); identity.groups.push("mutated"); identity.info.changed = true;
  connections[0].close(); await scheduledReady.promise; await assert.rejects(peer.call("session.list", {}), /not connected/); assert.equal(scheduled[0].milliseconds, 2000);
  const offline = { name: "offline title", info: { phase: "stored" } }; await assert.rejects(peer.rehello(undefined, offline.name, offline.info), /not connected/); offline.info.phase = "mutated";
  scheduledReady = deferred(); scheduled.shift().call(); await peer.ready; assert.equal(currentIdentity.name, "offline title"); assert.deepEqual(currentIdentity.groups, ["shared"]); assert.deepEqual(currentIdentity.info, { phase: "stored" });
  const beforeInvalid = structuredClone(peer.identity);
  for (const invalid of [{ name: "", info: {} }, { name: "changed", info: [] }]) await assert.rejects(peer.rehello(undefined, invalid.name, invalid.info), /invalid rehello identity/);
  assert.deepEqual(peer.identity, beforeInvalid);
  assert.equal((await connections[1].call("message.deliver", delivery)).disposition, "injected"); assert.equal(deliveries, 1);
  const renamed = { name: "renamed", info: {} }; await peer.rehello(undefined, renamed.name, renamed.info); assert.equal(currentIdentity.name, "renamed"); renamed.info.changed = true;
  holdHello = deferred(); const crossed = peer.rehello(undefined, "crossed title", { phase: "new" }); await holdHello.promise; connections[1].close(); holdHello = null;
  await assert.rejects(crossed, /not connected/); await scheduledReady.promise; scheduledReady = deferred(); scheduled.shift().call(); await peer.ready;
  assert.equal(currentIdentity.name, "crossed title"); assert.deepEqual(currentIdentity.info, { phase: "new" });
  const beforeTerminal = structuredClone(peer.identity);
  // The daemon sends this as its final frame and does not wait for an ACK;
  // immediate client closure may end the fixture's best-effort reply first.
  await connections[2].call("session.superseded", {}).catch((error) => assert.equal(error.message, "sessionbus connection closed"));
  await peer.closed;
  assert.equal(peer.error instanceof ProtocolError, true); assert.equal(peer.error.code, -32012);
  await assert.rejects(peer.rehello(undefined, "too late", {}), /superseded/); assert.deepEqual(peer.identity, beforeTerminal); assert.equal(scheduled.length, 0);
  await assert.rejects(peer.replace({ ...beforeTerminal, session_id: "too-late" }), /superseded/); assert.deepEqual(peer.identity, beforeTerminal);
}

for (const mode of ["peer", "worker"]) test(`${mode} written and uncertain native submission`, async (t) => {
  const admit = async (_signal, request) => {
    if (["complete write", "native EOF after complete write"].includes(request.body)) return { disposition: "written" };
    if (request.body === "pre-write failure") throw new Error(request.body);
    if (request.body === "observed native refusal") return { disposition: "rejected", reason: request.body };
    throw new ProtocolError({ code: -32603, message: "internal", data: `${request.body}; submission uncertain` });
  };
  let daemon;
  if (mode === "worker") {
    const product = new FakeProduct(); product.deliver = admit; product.started = deferred(); product.release = deferred(); ({ daemon } = await harness(t, product));
    const running = daemon.call("turn.run", { ...target, input: "block" }); await product.started.promise;
    t.after(async () => { product.release.resolve(); await running.catch(() => {}); });
  } else {
    const endpoint = peerEndpoint(); daemon = endpoint.daemon;
    const peer = connectPeer({ product: "native-product", session_id: "target", groups: [], info: {} }, admit, { SESSIONBUS_SOCKET: "/fixture/socket" }, { connect: () => endpoint.client, schedule: () => {} });
    t.after(() => { peer.shutdown(); daemon.close(); });
    const hello = await endpoint.next(); assert.equal(Object.hasOwn(hello.params, "name"), false);
    await daemon.result(hello, {}); await peer.ready;
  }
  for (const body of ["complete write", "native EOF after complete write", "pre-write failure", "observed native refusal", "partial write", "post-submission transport loss"]) {
    const result = daemon.call("message.deliver", { ...delivery, body });
    if (["partial write", "post-submission transport loss"].includes(body)) await assert.rejects(result, (error) => error instanceof ProtocolError && error.code === -32603 && error.data === `${body}; submission uncertain`);
    else assert.deepEqual(await result, ["complete write", "native EOF after complete write"].includes(body) ? { disposition: "written" } : { disposition: "rejected", reason: body });
  }
});

test("unnamed peer first title, removal and written delivery retain captured identity", async (t) => {
  const endpoint = peerEndpoint(), entered = deferred(), release = deferred();
  const initial = { product: "native-product", session_id: "old", groups: ["shared"], info: {} };
  const peer = connectPeer(initial, async (signal, request, identity) => { entered.resolve({ request: structuredClone(request), identity: structuredClone(identity), signal }); await release.promise; return { disposition: "written" }; }, { SESSIONBUS_SOCKET: "/fixture/socket" }, { connect: () => endpoint.client, schedule: () => {} });
  t.after(() => { peer.shutdown(); endpoint.daemon.close(); });
  await endpoint.daemon.result(await endpoint.next(), {}); await peer.ready;
  await assert.rejects(peer.rehello(undefined, "", {}), /invalid rehello/);
  for (const name of ["first title", undefined]) {
    const changed = peer.rehello(undefined, name, {}), hello = await endpoint.next();
    assert.equal(hello.params.session_id, initial.session_id); assert.equal(Object.hasOwn(hello.params, "name"), name !== undefined); assert.equal(hello.params.name, name);
    await endpoint.daemon.result(hello, {}); await changed;
  }
  const request = { message_id: "captured", from: { session_id: "source@local", product: "native-product", groups: ["shared"] }, body: "complete frame" };
  const delivering = endpoint.daemon.call("message.deliver", request), captured = await entered.promise;
  const replacing = peer.replace({ ...initial, session_id: "new", name: "replacement" });
  await endpoint.daemon.result(await endpoint.next(), {}); await replacing;
  release.resolve(); assert.deepEqual(await delivering, { disposition: "written" });
  assert.deepEqual(captured.identity, initial); assert.deepEqual(captured.request, request); assert.equal(captured.signal.aborted, true);
});

test("worker joins in-flight Open and exactly one successful Close after EOF", async (t) => {
  for (const fails of [false, true]) await t.test(fails ? "failed-open-cleanup" : "late-success", async (t) => {
    const product = new FakeProduct(), entered = deferred(), release = deferred();
    product.closeStart = deferred(); product.closeEnd = deferred();
    product.open = async () => { product.calls[1]++; entered.resolve(); await release.promise; if (fails) throw new Error("native Open failed after cleanup"); return { session_id: "product-session" }; };
    const { worker, daemon, serving } = await harness(t, product, { open: false });
    // Observe actual Serve cleanup without changing its adoption or scheduling.
    const cleanupEntered = deferred(), closeProduct = worker._closeProduct.bind(worker);
    worker._closeProduct = (...args) => { cleanupEntered.resolve(); return closeProduct(...args); };
    let finished = false; serving.then(() => { finished = true; });
    t.after(() => { release.resolve(); product.closeEnd.resolve(); });
    const opening = daemon.call("session.open", openRequest).catch((error) => error);
    await entered.promise; daemon.close(); await cleanupEntered.promise;
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(finished, false, "serve finished while native Open was still in flight");
    release.resolve();
    if (!fails) { await product.closeStart.promise; assert.equal(finished, false, "serve finished before product.Close"); }
    product.closeEnd.resolve(); await serving; await opening;
    assert.equal(product.calls[5], fails ? 0 : 1);
  });
});
