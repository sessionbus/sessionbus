// SPDX-License-Identifier: MIT
"use strict";

const test = require("node:test"), assert = require("node:assert/strict");
const { Connection, Transfers } = require("./index.js");
const { pair } = require("./test-support.js");

const identity = { session_id: "source@local", product: "test", groups: [], info: {} };
const offer = { target: "destination@local", source: "/local/source", name: "file" };
const descriptor = { id: "one", sender: "source@local", receiver: "destination@local", created_by: "source", name: "file", state: "offered", bytes: 0 };

function rawFixture(t, capabilities = ["transfer/1"], transfers = new Transfers()) {
  const [one, two] = pair(), scope = new AbortController(), incoming = [];
  let client, bus;
  client = new Connection(one, true, request => {
    if (!transfers.handleConnectionRequest(client, request)) throw new Error("unexpected callback");
  });
  bus = new Connection(two, false, request => {
    incoming.push(request);
    const result = request.method === "session.hello" ? (capabilities ? { capabilities } : {}) : descriptor;
    void bus.result(request, result).catch(() => bus.close());
  });
  t.after(async () => {
    scope.abort(); client.close(); bus.close();
    await transfers.closeConnectionGeneration(client);
  });
  return { transfers, client, bus, scope, incoming };
}

test("raw hello observer can synchronously offer on its admitted transfer generation", { timeout: 2000 }, async t => {
  const f = rawFixture(t); let offered;
  await f.transfers.helloObserved(f.client, identity, f.scope.signal, undefined, () => {
    offered = f.transfers.offer(offer);
  });
  assert.deepEqual(await offered, descriptor);
  assert.deepEqual(f.incoming.map(request => request.method), ["session.hello", "transfer.offer"]);
  assert.equal(f.transfers.pending, 1);
});

test("raw legacy hello observes ordinary readiness without enabling transfers", { timeout: 2000 }, async t => {
  const f = rawFixture(t, null); let seen = 0;
  await f.transfers.helloObserved(f.client, identity, f.scope.signal, undefined, () => {
    seen++;
    assert.throws(() => f.transfers.offer(offer), /unsupported_transfer/);
  });
  assert.equal(seen, 1); assert.equal(f.transfers.current, null);
  assert.deepEqual(f.incoming.map(request => request.method), ["session.hello"]);
});

test("raw hello does not publish readiness for a cancelled native identity", { timeout: 2000 }, async t => {
  const f = rawFixture(t), reason = new Error("identity ended"); let seen = 0;
  f.scope.abort(reason);
  await assert.rejects(f.transfers.helloObserved(f.client, identity, f.scope.signal, undefined, () => { seen++; }), error => error === reason);
  assert.equal(seen, 0); assert.equal(f.transfers.generations.size, 0);
});

test("rejected raw hello observer retires its captured transfer generation", { timeout: 2000 }, async t => {
  const f = rawFixture(t), reason = new Error("observer refused"); let captured, offered;
  await assert.rejects(f.transfers.helloObserved(f.client, identity, f.scope.signal, undefined, () => {
    captured = f.transfers.current;
    offered = f.transfers.offer(offer); void offered.catch(() => {});
    throw reason;
  }), error => error === reason);
  assert.ok(captured); await Promise.allSettled([offered]); await captured.finished.promise;
  assert.equal(captured.controller.signal.aborted, true);
  assert.equal(f.transfers.current, null); assert.equal(f.transfers.pending, 0); assert.equal(f.transfers.active, 0);
});

test("rejected raw hello observer cannot retire its synchronous successor", { timeout: 2000 }, async t => {
  const first = rawFixture(t), next = rawFixture(t, ["transfer/1"], first.transfers);
  const reason = new Error("old observer refused"); let captured, successor;
  await assert.rejects(first.transfers.helloObserved(first.client, identity, first.scope.signal, undefined, () => {
    captured = first.transfers.current;
    successor = first.transfers.attach(next.client, next.scope.signal, true);
    throw reason;
  }), error => error === reason);
  assert.ok(captured); await captured.finished.promise;
  await first.transfers.closeConnectionGeneration(first.client);
  assert.ok(successor); assert.equal(first.transfers.current, successor);
  assert.equal(successor.controller.signal.aborted, false);
  assert.deepEqual(await next.transfers.offer(offer), descriptor);
  assert.deepEqual(next.incoming.map(request => request.method), ["transfer.offer"]);
});
