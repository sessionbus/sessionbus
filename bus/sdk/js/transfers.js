// SPDX-License-Identifier: MIT
"use strict";

const { createHash } = require("node:crypto");
const { basename } = require("node:path");
const { CAPABILITY, CHUNK_BYTES, CREDIT_BYTES } = require("./transfer-protocol.js");
const { validate } = require("./schema.js");
const { FilesystemTransferExecutor } = require("./transfer-filesystem.js");

// Handles are endpoint-local. With no custom executor, the SDK reads/writes
// directly as the OS user in every product/mode. Custom async methods honour
// cancellation and the same aggregate four filesystem-FD budget.
const ErrTransferCommitUnknown = new Error("transfer_commit_unknown");
const unsupported = () => new Error("unsupported_transfer");
const conflict = () => new Error("transfer_binding_conflict");
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };
function waitSignal(promise, signal) {
  if (!signal) return promise;
  if (signal.aborted) { promise.catch(() => {}); return Promise.reject(signal.reason || new Error("aborted")); }
  return new Promise((resolve, reject) => {
    const abort = () => { signal.removeEventListener("abort", abort); reject(signal.reason || new Error("aborted")); };
    signal.addEventListener("abort", abort, { once: true });
    promise.then(value => { signal.removeEventListener("abort", abort); resolve(value); }, error => { signal.removeEventListener("abort", abort); reject(error); });
  });
}
function pathValid(path) { return typeof path === "string" && Buffer.byteLength(path) > 0 && Buffer.byteLength(path) <= 4096; }
function binding(role, path = "", overwrite = false) {
  return { role, path, overwrite, active: false, offset: 0, size: 0, tail: Promise.resolve(), retired: deferred(), commitFinished: deferred() };
}
function locked(b, callback) {
  const result = b.tail.then(callback);
  b.tail = result.catch(() => {});
  return result;
}

class Transfers {
  constructor(executor = new FilesystemTransferExecutor()) {
    executor ??= new FilesystemTransferExecutor();
    if (!executor || typeof executor.openTransferSource !== "function" || typeof executor.openTransferDestination !== "function") throw unsupported();
    this.executor = executor; this.pending = 0; this.active = 0; this.generations = new Set();
  }
  attach(wire, signal, enabled) {
    if (this.current?.wire === wire && this.current.enabled === enabled && !this.current.controller.signal.aborted) return;
    this.current?.controller.abort();
    if (!enabled) { this.current = null; return; }
    const controller = new AbortController();
    const g = { wire, controller, enabled, bindings: new Map(), creating: new Set(), finished: deferred() };
    this.current = g; this.generations.add(g);
    const abort = () => controller.abort(signal?.reason || wire.signal.reason);
    signal?.addEventListener("abort", abort, { once: true }); wire.signal.addEventListener("abort", abort, { once: true });
    controller.signal.addEventListener("abort", () => {
      signal?.removeEventListener("abort", abort); wire.signal.removeEventListener("abort", abort);
      void (async () => {
        const all = [...g.bindings.values()];
        await Promise.all(all.map(b => this._end(g, b)));
        await Promise.all(all.map(b => b.retired.promise));
        await Promise.allSettled([...g.creating]);
        this.generations.delete(g); g.finished.resolve();
      })();
    }, { once: true });
    if (signal?.aborted || wire.signal.aborted) abort();
  }
  _generation() {
    const g = this.current;
    if (!g?.enabled || g.controller.signal.aborted) throw unsupported();
    return g;
  }
  async helloObserved(wire, identity, generationSignal, signal, seen) {
    const hello = { protocol: 1, ...identity, info: { ...identity.info, sessionbus_transfer: 1 } };
    return wire.call("session.hello", hello, signal, result => {
      seen?.(); this.attach(wire, generationSignal, result.capabilities?.includes(CAPABILITY) === true);
    });
  }
  _reserve(g, b, hasPath = true) {
    if (g.controller.signal.aborted || hasPath && !pathValid(b.path)) throw conflict();
    if (this.pending >= 8) throw new Error("transfer_busy");
    this.pending++;
    b.controller = new AbortController();
    b.abort = () => b.controller.abort(g.controller.signal.reason);
    g.controller.signal.addEventListener("abort", b.abort, { once: true });
    if (g.controller.signal.aborted) b.abort();
    if (b.role === "destination" && b.path) b.destinationProjection = b.path;
  }
  _create(method, params, b, signal) {
    if (signal?.aborted) return Promise.reject(signal.reason || new Error("aborted"));
    const g = this._generation(); this._reserve(g, b);
    let installed = false;
    const creation = g.wire.call(method, params, g.controller.signal, result => {
      if (g.controller.signal.aborted || g.bindings.has(result.id)) throw conflict();
      b.id = result.id; g.bindings.set(b.id, b); installed = true;
    }).finally(() => {
      if (!installed) { this.pending--; g.controller.signal.removeEventListener("abort", b.abort); b.retired.resolve(); }
      g.creating.delete(creation);
    });
    g.creating.add(creation);
    // Caller cancellation stops waiting, not the sole generation-owned request.
    return waitSignal(creation, signal);
  }
  offer({ target, source, name }, signal) { return this._create("transfer.offer", { target, name: name || (typeof source === "string" ? basename(source) : "") }, binding("source", source), signal); }
  request({ target, what, destination, name, overwrite = false }, signal) {
    return this._create("transfer.request", { target, what, ...(name === undefined ? {} : { name }) }, binding("destination", destination, overwrite), signal);
  }
  accept({ id, path, overwrite = false }, signal) {
    if (signal?.aborted) return Promise.reject(signal.reason || new Error("aborted"));
    if (!pathValid(path)) return Promise.reject(conflict());
    const g = this._generation(), b = g.bindings.get(id);
    if (!b || b.ended || b.role === "source" && overwrite || b.path && (b.path !== path || b.overwrite !== overwrite)) return Promise.reject(conflict());
    b.path = path; b.overwrite = overwrite;
    if (b.role === "destination") b.destinationProjection = path;
    return waitSignal(g.wire.call("transfer.accept", { id }, g.controller.signal), signal);
  }
  status({ id }, signal) { return this._generation().wire.call("transfer.status", { id }, signal); }
  cancel({ id }, signal) { return this._generation().wire.call("transfer.cancel", { id }, signal); }
  action(action, args, signal) {
    const fields = { "transfer.offer": ["target", "source", "name"], "transfer.request": ["target", "what", "destination", "name", "overwrite"], "transfer.accept": ["id", "source", "destination", "overwrite"], "transfer.cancel": ["id"], "transfer.status": ["id"] }[action];
    if (!fields || !args || typeof args !== "object" || Array.isArray(args) || Object.keys(args).some(key => !fields.includes(key))) return Promise.reject(conflict());
    if (Object.hasOwn(args,"overwrite") && typeof args.overwrite !== "boolean") return Promise.reject(conflict());
    if (action === "transfer.accept") {
      const b = this._generation().bindings.get(args.id);
      if (!b || Boolean(args.source) === Boolean(args.destination) || b.role === "source" && args.overwrite) return Promise.reject(conflict());
      return this.accept({ id: args.id, path: b.role === "source" ? args.source : args.destination, overwrite: args.overwrite }, signal);
    }
    return this[action.slice(9)](args,signal);
  }
  handleConnectionRequest(wire, request) {
    if (!["transfer.prepare", "transfer.start", "transfer.chunk", "transfer.finish", "transfer.notice"].includes(request.method)) return false;
    const g = this.current;
    // Queue callbacks off the shared reader, never filesystem work in an observer.
    void Promise.resolve().then(async () => {
      if (!g?.enabled || g.wire !== wire || g.controller.signal.aborted) { await wire.error(request, -32008, "unsupported_transfer"); return; }
      try { await wire.result(request, await this._handle(g, request)); }
      catch { await wire.error(request, -32603, "endpoint_io_failed"); }
    }).catch(() => wire.close());
    return true;
  }
  async _handle(g, request) {
    const p = request.params;
    if (request.method === "transfer.notice") {
      let b = g.bindings.get(p.id);
      if (p.kind === "terminal") {
        if (b) {
          await this._end(g, b); await b.worker;
          if (b.cleanupError) throw b.cleanupError;
          await b.retired.promise;
        }
      } else if (!b) {
        b = binding(p.kind === "request" ? "source" : "destination");
        this._reserve(g, b, false); b.id = p.id; g.bindings.set(b.id, b);
      }
      return {};
    }
    const b = g.bindings.get(p.id); if (!b) throw conflict();
    return locked(b, async () => {
      if (b.ended) throw conflict();
      switch (request.method) {
        case "transfer.prepare": {
          if (b.role !== p.role || !pathValid(b.path)) throw conflict();
          if (b.prepareAttempted) { if (b.prepareError) throw b.prepareError; return this._prepared(b); }
          b.prepareAttempted = true;
          try {
            if (this.active >= 2 || g.controller.signal.aborted) throw conflict();
            this.pending--; this.active++; b.active = true;
            if (b.role === "source") {
              b.source = await this.executor.openTransferSource(b.controller.signal, b.path);
              b.size = await b.source.size();
              if (!Number.isSafeInteger(b.size) || b.size < 0 || b.size > (1 << 30)) throw conflict();
            } else {
              if (p.size === undefined) throw conflict();
              b.size = p.size; b.destination = await this.executor.openTransferDestination(b.controller.signal, b.path, b.overwrite); b.hash = createHash("sha256");
            }
            b.prepared = true; return this._prepared(b);
          } catch (error) { b.prepareError = error; throw error; }
        }
        case "transfer.start": {
          if (!b.prepared || b.role !== p.role || b.size !== p.size || b.role === "source" && p.credit !== CREDIT_BYTES) throw conflict();
          if (!b.started) { b.started = true; if (b.role === "source") b.worker = Promise.resolve().then(() => this._stream(g, b)); }
          return { id: b.id, [b.role === "source" ? "started" : "ready"]: true };
        }
        case "transfer.chunk": {
          if (!b.started || b.role !== "destination" || p.offset !== b.offset) throw conflict();
          const raw = Buffer.from(p.data, "base64");
          if (!raw.length || raw.length > CHUNK_BYTES || raw.toString("base64") !== p.data || b.offset + raw.length > b.size) throw conflict();
          if (await b.destination.write(raw, b.controller.signal) !== raw.length) throw new Error("short_write");
          b.hash.update(raw); b.offset += raw.length; return { offset: b.offset };
        }
        case "transfer.finish": {
          if (!b.started || b.finishAttempted || b.role !== "destination" || b.offset !== b.size || b.offset !== p.bytes || b.hash.copy().digest("hex") !== p.sha256) throw conflict();
          b.finishAttempted = true; b.commitStarted = true;
          let result;
          try { result = await b.destination.commit(); }
          catch (error) { result = { published: false, error: error === ErrTransferCommitUnknown ? error : ErrTransferCommitUnknown }; }
          finally { /* Resolve only after storing the publication fact below. */ }
          try {
            if (!result || typeof result.published !== "boolean") result = { published: false, error: ErrTransferCommitUnknown };
            b.published = result.published;
            if (!b.published && result.error === ErrTransferCommitUnknown) { b.commitUnknown = true; return { id: b.id, outcome: "unknown", bytes: b.offset, reason: "publication_unconfirmed" }; }
            if (!b.published) return { id: b.id, outcome: "failed", bytes: b.offset, reason: "publication_failed" };
            b.publication = { id: b.id, outcome: "completed", bytes: b.offset, sha256: p.sha256, ...(result.error ? { reason: "committed_cleanup_failed" } : {}) };
            return b.publication;
          } finally { b.commitFinished.resolve(); }
        }
        default: throw conflict();
      }
    });
  }
  _prepared(b) { return { id: b.id, ...(b.role === "source" ? { size: b.size } : { ready: true }) }; }
  async _stream(g, b) {
    let sourceFailure;
    try {
      const source = b.source, digest = createHash("sha256"), buffer = Buffer.alloc(CHUNK_BYTES);
      let offset = 0;
      while (offset < b.size) {
        const piece = buffer.subarray(0, Math.min(CHUNK_BYTES, b.size - offset));
        let read = 0;
        while (read < piece.length) { sourceFailure = "source_read_failed"; const n = await source.read(piece.subarray(read), b.controller.signal); if (!Number.isSafeInteger(n) || n <= 0 || n > piece.length - read) throw conflict(); read += n; }
        sourceFailure = undefined;
        digest.update(piece);
        const credit = await g.wire.call("transfer.chunk", { id: b.id, offset, data: piece.toString("base64") }, b.controller.signal);
        if (credit.offset !== offset + piece.length) throw conflict();
        offset += piece.length;
      }
      sourceFailure = "source_changed";
      await source.verify();
      sourceFailure = undefined;
      await g.wire.call("transfer.finish", { id: b.id, bytes: offset, sha256: digest.digest("hex") }, b.controller.signal);
    } catch {
      if (!b.ended && !g.controller.signal.aborted) {
        if (sourceFailure) { try { await g.wire.call("transfer.fail", { id: b.id, reason: sourceFailure }, g.controller.signal); } catch {} }
        // Do not await retirement from the worker that retirement itself joins.
        void this._end(g, b);
      }
    }
  }
  async _end(g, b) {
    if (!b.commitStarted) b.controller.abort();
    await locked(b, async () => {
      if (b.ended) return;
      b.ended = true;
      try { await b.source?.close(); if (b.commitUnknown) throw ErrTransferCommitUnknown; await b.destination?.abort(); }
      catch (error) { b.cleanupError = error; }
      b.source = undefined; b.destination = undefined;
      void (async () => {
        await b.worker;
        if (b.cleanupError && !g.controller.signal.aborted) await new Promise(resolve => g.controller.signal.addEventListener("abort", resolve, { once: true }));
        if (g.bindings.get(b.id) === b) {
          g.bindings.delete(b.id); if (b.active) this.active--; else this.pending--;
          g.controller.signal.removeEventListener("abort", b.abort); b.retired.resolve();
        }
      })();
    });
  }
  projectDelivery(wire, delivery) {
    if (!delivery.message_id?.startsWith("transfer-notice-")) return undefined;
    let notice; try { notice = JSON.parse(delivery.body); } catch { return undefined; }
    if (!validate("TransferNotice", notice) || notice.kind !== "terminal" || this.current?.wire !== wire) return undefined;
    const b = this.current.bindings.get(notice.id), path = b?.destinationProjection;
    if (!path) return undefined;
    return async (signal, out) => {
      if (b.commitStarted) await waitSignal(b.commitFinished.promise, signal);
      const fact = b.publication;
      out.body = JSON.stringify({ ...notice, ...(fact ? { outcome: fact.outcome, bytes: fact.bytes, sha256: fact.sha256, reason: fact.reason } : {}), destination: path });
    };
  }
  async closeConnectionGeneration(wire) {
    const all = [...this.generations].filter(g => !wire || g.wire === wire);
    for (const g of all) g.controller.abort();
    await Promise.all(all.map(g => g.finished.promise));
  }
}

module.exports = { Transfers, ErrTransferCommitUnknown };
