// SPDX-License-Identifier: MIT

"use strict";

const net = require("node:net");
const { performance } = require("node:perf_hooks");
const { isDeepStrictEqual } = require("node:util");
const { ACTIONS, Caller } = require("./caller.js");
const { Connection, ProtocolError } = require("./connection.js");
const { schema, validate, encode } = require("./schema.js");

const ENV = ["SESSIONBUS_LAUNCH_TOKEN", "SESSIONBUS_LOCAL_KEY", "SESSIONBUS_SOCKET"];
const never = new Promise(() => {});

class Run {
  constructor(parent) {
    this.Native = null;
    this.interrupted = false;
    this.controller = new AbortController();
    const abort = () => this.controller.abort(parent.reason);
    if (parent?.aborted) abort(); else parent?.addEventListener("abort", abort, { once: true });
    this.finished = false;
    this.Done = new Promise((resolve) => { this.finish = () => { parent?.removeEventListener("abort", abort); this.finished = true; resolve(); }; });
    this.AdmittedDone = new Promise((resolve) => { this.admit = resolve; });
    this.admitted = false;
  }
  ReportDelivery(value, failure) {
    if (!this.receipt) return Promise.reject(new Error("run has no delivery seed"));
    if (this.receiptReported) return Promise.reject(new Error("delivery receipt already reported"));
    this.receiptReported = true;
    return this.receipt(value, failure);
  }
  Interrupted() { return this.interrupted; }
  Admitted() { if (!this.admitted) { this.admitted = true; this.admit(); } }
}

class Worker {
  constructor(callbacks, env = process.env, options = {}) {
    this.callbacks = callbacks;
    this.env = env;
    this.connect = options.connect || ((path) => net.createConnection(path));
    this.controller = new AbortController();
    this.run = null;
    this.opened = false;
    this.records = []; this.generation = ""; this.lastSequence = 0; this.acknowledged = 0; this.waiters = 0;
    this.changed = new Promise((resolve) => { this.changedResolve = resolve; });
    this.closeRequest = {};
    this.closed = new Promise((resolve) => { this.finish = resolve; });
    this.caller = new Caller(this, options.caller);
  }
  async serve() {
    let cause;
    try {
      const { socket, token } = environment(this.env, true);
      const hello = await this.callbacks.hello(this.controller.signal);
      this.supportsWake = hello.supports_message_run === true;
      const stream = this.connect(socket);
      this.connection = new Connection(stream, true, (request) => this._handle(request));
      this.connection.signal.addEventListener("abort", () => this.controller.abort(this.connection.signal.reason), { once: true });
      await this.connection.call("session.hello", { protocol: 1, launch_token: token, ...hello }, this.controller.signal); cause = await this.connection.done; throw cause;
    } finally { this.run?.finish?.(); try { await this._closeProduct(); } finally { this.finish(cause); } }
  }
  call(method, params, signal = this.controller.signal) { return this.connection.call(method, params, signal); }
  shutdown() { this.connection?.close(); }
  _handle(request) {
    if (request.method === "session.superseded") { void this.connection.result(request, {}).finally(() => this.shutdown()); return; }
    if (request.method === "session.open") {
      if (this.opening || this.connection.signal.aborted || this.run && !this.run.Done) { void this._replyError(request, -32600); return; }
      // Register before scheduling so EOF cleanup joins native Open adoption.
      this.opening = Promise.resolve().then(() => this._open(request)).catch((error) => { callbackError("open", error); this.shutdown(); });
      return;
    }
    if (request.method === "turn.execute") { this._execute(request); return; }
    if (request.method === "turn.status" || request.method === "turn.wait") { void this._read(request); return; }
    if (request.method === "turn.ack") { void this._ack(request); return; }
    if (request.method === "turn.interrupt") {
      if (!this.run) { void this.connection.error(request, -32004); return; }
      if (!this.run.Done) { void this.connection.result(request, {}); return; }
      if (this.run.controller.signal.aborted) { void this.connection.error(request, -32004); return; }
      const run = this.run, call = !run.interrupted; run.interrupted = true; queueMicrotask(() => void this._interrupt(request, run, call)); return;
    }
    if (request.method === "message.deliver") { if (request.params.run_id) { this._execute(request, request.params); return; } const run = this.run; if (run && !run.Done) void this.connection.result(request, { disposition: "rejected", reason: "closing" }); else queueMicrotask(() => void this._deliver(request, run)); return; }
    if (request.method === "session.close") {
      const run = this.run;
      if (run && !run.Done) return this.shutdown();
      this.closeRequest = request.params;
      this.run = {};
      const call = run && !run.controller.signal.aborted && !run.interrupted;
      if (call) run.interrupted = true;
      queueMicrotask(() => void this._close(request, run, call));
    }
  }
  async _open(request) {
    if (request.params.policy && !this.supportsWake) { await this._replyError(request, -32008); return; }
    let result; try { result = await this.callbacks.open(this.controller.signal, request.params); } catch (error) { await this._replyError(request, -32009, { stderr_tail: [clean(error)] }); return; }
    this.opened = true;
    const at = request.params.name.lastIndexOf("@");
    this.sessionID = result.session_id + (at < 0 ? "" : request.params.name.slice(at));
    try { await this.connection.result(request, result); } catch { this.shutdown(); }
  }
  _execute(request, delivery) {
    if (!this.opened || this.run || this.records.length >= 256) { void this._replyError(request, -32003); return; }
    const ref = { session_id: delivery ? this.sessionID : request.params.session_id, run_id: request.params.run_id };
    const parsed = runSequence(ref.run_id);
    if (ref.session_id !== this.sessionID || !parsed || this.generation && this.generation !== parsed.generation || parsed.sequence !== this.lastSequence + 1) { void this._replyError(request, -32600); return; }
    this.generation = parsed.generation; this.lastSequence = parsed.sequence;
    const run = new Run(this.controller.signal), record = { status: { ...ref, state: "running" }, sequence: parsed.sequence };
    let seed;
    if (delivery) { const copy = structuredClone(delivery); delete copy.run_id; seed = { delivery: copy }; run.receipt = (value, failure) => this._deliveryReply(request, value, failure); }
    else seed = { text: request.params.input };
    this.run = run; this.records.push(record);
    void (async () => {
      try { if (!delivery) await this.connection.result(request, ref); await this._run(run, record, seed); } catch { this.shutdown(); }
    })();
  }
  async _run(run, record, seed) {
    let status = { ...record.status, state: "done" };
    try {
      let result;
      try { result = await this.callbacks.run(run.controller.signal, run, seed); } finally { run.controller.abort(); }
      status.result = structuredClone(result);
    } catch (error) { status = { ...record.status, state: "unavailable", reason: clean(error) }; }
    try {
      if (status.state === "done") encode("TurnRunResult", status.result);
      encode("RunStatus", status);
      if (Buffer.byteLength(JSON.stringify({ jsonrpc: "2.0", id: Number.MAX_SAFE_INTEGER, result: status }) + "\n") > 1 << 20) throw new Error("frame too large");
    } catch { status = { ...record.status, state: "unavailable", reason: "native result failed validation" }; }
    if (seed.delivery) { try { await run.ReportDelivery(undefined, new ProtocolError({ code: -32603, message: "internal", data: "native delivery receipt unavailable" })); } catch {} }
    const ready = { session_id: status.session_id, run_id: status.run_id, state: status.state, ...(status.result ? { outcome: status.result.outcome } : { reason: status.reason }) };
    await this.connection.call("turn.ready", ready, this.controller.signal, () => {
      record.status = status; if (this.run === run) this.run = null;
      this.changedResolve(); this.changed = new Promise((resolve) => { this.changedResolve = resolve; }); run.finish();
    });
  }
  _find(request) { return this.records.find((record) => record.status.session_id === request.session_id && (!request.run_id || record.status.run_id === request.run_id)); }
  async _read(request) {
    if (this.waiters >= 256 || this.run && !this.run.Done) { await this._replyError(request, -32003); return; }
    this.waiters++;
    let timer, removeAbort;
    try {
      let timedOut = false;
      const timeout = request.params.timeout_ms === undefined ? never : new Promise((resolve) => {
        const deadline = performance.now() + request.params.timeout_ms;
        // Node clamps larger delays to 1ms. Preserve the explicit user deadline
        // with maximum-length timer segments; this is not RPC cancellation polling.
        const expire = () => { const remaining = deadline - performance.now(); if (remaining > 0) timer = setTimeout(expire, Math.min(remaining, 2147483647)); else { timedOut = true; resolve(); } };
        timer = setTimeout(expire, Math.min(request.params.timeout_ms, 2147483647));
      });
      const aborted = new Promise((resolve) => { const abort = () => resolve(); this.controller.signal.addEventListener("abort", abort, { once: true }); removeAbort = () => this.controller.signal.removeEventListener("abort", abort); });
      while (!this.controller.signal.aborted) {
        const record = this._find(request.params);
        if (!record) { await this._replyError(request, -32001); return; }
        if (request.method === "turn.status" || record.status.state !== "running" || timedOut) { await this.connection.result(request, record.status); return; }
        await Promise.race([this.changed, timeout, aborted]);
      }
    } catch { this.shutdown(); }
    finally { clearTimeout(timer); removeAbort?.(); this.waiters--; this.changedResolve(); this.changed = new Promise((resolve) => { this.changedResolve = resolve; }); }
  }
  async _ack(request) {
    const parsed = runSequence(request.params.run_id);
    let code = -32001;
    if (parsed && parsed.generation === this.generation && request.params.session_id === this.sessionID) {
      if (parsed.sequence <= this.acknowledged) code = 0;
      else if (this.records[0]?.status.run_id === request.params.run_id && this.records[0].status.state !== "running") { this.acknowledged = parsed.sequence; this.records.shift(); code = 0; }
      else if (this.records.length) code = -32003;
    }
    if (code) await this._replyError(request, code);
    else { try { await this.connection.result(request, {}); } catch { this.shutdown(); } }
  }
  async _interrupt(request, run, call) {
    try { if (call && !run.controller.signal.aborted) await this.callbacks.interrupt(run.controller.signal, run); } catch (error) { callbackError("interrupt", error); }
    try { await this.connection.result(request, {}); } catch { this.shutdown(); }
  }
  async _deliver(request, run) {
    if (!run || run.controller.signal.aborted) { await this._replyError(request, -32004); return; }
    let receipt, failure;
    try { receipt = await this.callbacks.deliver(this.controller.signal, request.params, undefined, run); } catch (error) { failure = error; }
    try { await this._deliveryReply(request, receipt, failure); } catch {}
  }
  async _deliveryReply(request, receipt, failure) {
    try {
      if (failure instanceof ProtocolError && failure.code === -32004) await this.connection.error(request, failure.code);
      else if (failure instanceof ProtocolError && failure.code === -32603) await this.connection.error(request, failure.code, failure.data);
      else await this.connection.result(request, failure ? { disposition: "rejected", reason: clean(failure) } : receipt);
    } catch (error) { this.shutdown(); throw error; }
  }
  async _close(request, run, interrupt) {
    if (interrupt) void Promise.resolve().then(() => { if (!run.controller.signal.aborted) return this.callbacks.interrupt(run.controller.signal, run); }).catch((error) => callbackError("interrupt", error));
    if (run) {
      await Promise.race([run.Done, this.connection.done]);
      if (this.connection.signal.aborted) { run.finish(); return; }
    }
    while (this.waiters && !this.connection.signal.aborted) await this.changed;
    this.controller.abort();
    await this._closeProduct(this.connection.signal);
    try { await this.connection.result(request, {}); } catch { this.shutdown(); } finally { this.shutdown(); }
  }
  async _closeProduct(signal = this.controller.signal) { await this.opening; if (!this.opened) return; if (!this.productClose) this.productClose = Promise.resolve().then(() => this.callbacks.close(signal, this.closeRequest)).catch((error) => callbackError("close", error)); await this.productClose; }
  async _replyError(request, code, data) { try { await this.connection.error(request, code, data); } catch { this.shutdown(); } }
}

class Peer {
  constructor(identity, deliver, env, options) {
    this.identity = snapshot(identity);
    this.deliver = deliver;
    this.connect = options.connect || ((path) => net.createConnection(path));
    this.schedule = options.schedule || ((call) => setTimeout(call, 2000));
    this.socket = environment(env, false).socket;
    this.terminal = false;
    this.error = null;
    this.admitted = null;
    this.identityController = null;
    this.closed = new Promise((resolve) => { this.finish = resolve; });
    this.ready = this._open();
    this.caller = new Caller(this, options.caller);
  }
  call(method, params, signal) { if (!this.connection) return Promise.reject(new Error("not connected")); return this.connection.call(method, params, signal); }
  rehello(signal, name, info) { return this._change({ name, info }, false, signal); }
  replace(identity) { return this._change(identity, true); }
  async _change(update, replacement, signal) {
    if (this.terminal) throw new Error("superseded");
    const object = update && typeof update === "object" && !Array.isArray(update), desired = replacement ? update : { ...this.identity, name: update?.name, info: update?.info };
    if (!replacement && desired.name === undefined) delete desired.name;
    if (!object || !replacement && (Object.keys(update).length !== 2 || !Object.hasOwn(update, "name") || !Object.hasOwn(update, "info")) || !validate("SessionHelloRequest", { protocol: 1, ...desired }) || replacement && update.session_id === this.identity.session_id) throw new Error(`invalid ${replacement ? "replace" : "rehello"} identity`);
    const next = snapshot(desired);
    if (!replacement && isDeepStrictEqual(next, this.identity)) return;
    const connection = this.connection;
    this.identity = next;
    const identity = this.identity;
    if (replacement) { this.identityController?.abort(new Error("not connected")); this.identityController = null; this.admitted = null; this.connection = null; this.caller.disconnected(); }
    if (!connection || connection.signal.aborted) throw new Error("not connected");
    let acknowledged = false;
    const changed = (async () => {
      try { const result = await this._hello(connection, identity, () => { acknowledged = true; }); if (replacement && (this.wire !== connection || connection.signal.aborted)) throw new Error("not connected"); return result; }
      catch (error) { if (this._failHello(error, connection)) throw error; if (connection.signal.aborted) throw new Error("not connected"); if (replacement) connection.close(); throw error; }
    })();
    if (!signal) return changed;
    return callerContext(changed, signal, () => acknowledged);
  }
  shutdown() { this.terminal = true; this.connection = null; this.identityController?.abort(new Error("not connected")); this.caller.disconnected(); this.wire?.close(); this.finish(); }
  async _open() {
    if (this.terminal) return; let connection; try { connection = new Connection(this.connect(this.socket), true, (request) => this._handle(request, connection)); } catch { this.schedule(() => { this.ready = this._open(); }, 2000); return; } this.wire = connection;
    try { await this._hello(connection); } catch (error) { if (this._failHello(error, connection)) return never; connection.close(); }
    if (!connection.signal.aborted) void connection.done.then(() => this._lost(connection)); else this._lost(connection);
  }
  async _hello(connection, identity = this.identity, acknowledged) { for (;;) { let installed = false; const result = await connection.call("session.hello", { protocol: 1, ...identity }, undefined, () => {
    if (this.wire !== connection || this.terminal) throw new Error("not connected");
    if (identity === this.identity) { if (this.admitted?.session_id !== identity.session_id || !this.identityController || this.identityController.signal.aborted) this.identityController = new AbortController(); this.admitted = identity; this.connection = connection; installed = true; }
  }); if (installed) { acknowledged?.(); return result; } identity = this.identity; } }
  _lost(connection) { if (this.wire !== connection || this.terminal) return; this.wire = null; this.connection = null; this.admitted = null; this.identityController?.abort(connection.signal.reason); this.identityController = null; this.caller.disconnected(); this.schedule(() => { this.ready = this._open(); }, 2000); }
  _failHello(error, connection) { if (!(error instanceof ProtocolError) || error.code !== -32602) return false; this.terminal = true; this.error = error; this.wire = null; this.connection = null; this.identityController?.abort(error); this.identityController = null; this.admitted = null; this.caller.disconnected(); connection.close(); this.finish(); return true; }
  _handle(request, connection) {
    if (request.method === "session.superseded") {
      this.terminal = true; this.error = new ProtocolError({ code: -32012, message: "superseded" });
      this.connection = null; this.caller.disconnected();
      this.identityController?.abort(this.error);
      // The daemon closes after its final frame without waiting for a reply.
      // Attempt the courtesy response, but its write cannot hold terminal cleanup.
      void connection.result(request, {}).catch(() => {});
      connection.close(); this.finish();
      return;
    }
    if (request.method === "message.deliver") { const current = this.connection === connection && this.admitted && this.identityController && !this.identityController.signal.aborted; const admission = current ? { identity: snapshot(this.admitted), signal: this.identityController.signal } : null; void Promise.resolve().then(() => admission ? this.deliver(admission.signal, request.params, admission.identity) : { disposition: "rejected", reason: "closing" }).then((result) => connection.result(request, result), (error) => error instanceof ProtocolError && error.code === -32603 ? connection.error(request, error.code, error.data) : connection.result(request, { disposition: "rejected", reason: clean(error) })).catch(() => connection.close()); }
  }
}

function connectPeer(identity, deliver, env = process.env, options = {}) { return new Peer(identity, deliver, env, options); }
function serveWorker(callbacks, env = process.env, options = {}) { const worker = new Worker(callbacks, env, options); worker.serving = worker.serve().catch((error) => error); return worker; }
function callerContext(promise, signal, acknowledged) {
  if (signal.aborted && !acknowledged()) { promise.catch(() => {}); return Promise.reject(signal.reason || new Error("aborted")); }
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (call, value) => { if (!settled) { settled = true; signal.removeEventListener("abort", abort); call(value); } };
    const abort = () => { if (!acknowledged()) finish(reject, signal.reason || new Error("aborted")); };
    signal.addEventListener("abort", abort, { once: true });
    promise.then((value) => finish(resolve, value), (error) => finish(reject, error));
  });
}
function snapshot(identity) { return { ...identity, groups: [...identity.groups], info: structuredClone(identity.info) }; }
function environment(env, worker) { const values = Object.fromEntries(ENV.map((name) => [name, env[name]])); for (const name of ENV) delete env[name]; if (!values.SESSIONBUS_SOCKET) throw new Error("sessionbus socket is required"); if (values.SESSIONBUS_LOCAL_KEY) throw new Error("local key transport not implemented in this build"); if (worker && !values.SESSIONBUS_LAUNCH_TOKEN) throw new Error("launch token is required"); return { socket: values.SESSIONBUS_SOCKET, token: values.SESSIONBUS_LAUNCH_TOKEN }; }
function runSequence(id) { const match = /^([^/]+)\/([1-9][0-9]*)$/.exec(id); if (!match) return null; const sequence = Number(match[2]); return Number.isSafeInteger(sequence) ? { generation: match[1], sequence } : null; }
function clean(error) { return String(error?.message || error || "product callback failed"); }
function callbackError(callback, error) { if (error) process.stderr.write(`sessionbus: product ${callback}: ${JSON.stringify(clean(error))}\n`); }

module.exports = { ACTIONS, Caller, connectPeer, Connection, ENV, Peer, ProtocolError, Run, serveWorker, Worker, schema, validate };
