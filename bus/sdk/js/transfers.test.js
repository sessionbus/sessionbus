// SPDX-License-Identifier: MIT
"use strict";
const test = require("node:test"), assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const { Connection, Caller, Worker, Transfers, ErrTransferCommitUnknown } = require("./index.js");
const { pair, deferred } = require("./test-support.js");
const sourceText = Buffer.from("checked source, never a model message"), hash = createHash("sha256").update(sourceText).digest("hex");
function fixture(t, executor, handler = async () => ({})) {
  const [one, two] = pair(), scope = new AbortController();
  const transfers = new Transfers(executor), incoming = [];
  let client, bus;
  client = new Connection(one, true, request => {
    if (!transfers.handleConnectionRequest(client, request)) throw new Error("unexpected callback");
  });
  bus = new Connection(two, false, request => {
    incoming.push(request);
    void Promise.resolve().then(() => handler(request)).then(result => bus.result(request, result)).catch(() => bus.close());
  });
  transfers.attach(client, scope.signal, true);
  t.after(async () => { scope.abort(); client.close(); bus.close(); await transfers.closeConnectionGeneration(); });
  return { transfers, bus, client, incoming, scope };
}
const noExecutor = { openTransferSource: async () => { throw new Error("no grant"); }, openTransferDestination: async () => { throw new Error("no grant"); } };
function descriptor(id, created_by = "source") { return { id, sender: "source@local", receiver: "destination@local", created_by, name: "file", state: created_by === "source" ? "offered" : "requested", bytes: 0 }; }
async function incomingDestination(f, id = "one") {
  await f.bus.call("transfer.notice", { kind: "offer", id, name: "file", from: { session_id: "source@local", product: "test", groups: [] } });
  await f.transfers.accept({ id, path: "/local/destination" });
  assert.deepEqual(await f.bus.call("transfer.prepare", { id, role: "destination", size: sourceText.length }), { id, ready: true });
  await f.bus.call("transfer.start", { id, role: "destination", size: sourceText.length });
}

test("local offer action binds the sole creation reply; chunks/hash pass, paths never reach wire", { timeout: 2000 }, async t => {
  let opened = 0, reads = 0, closed = 0; const chunks = [], finished = deferred();
  const executor = { ...noExecutor, openTransferSource: async (_signal, path) => {
    opened++; assert.equal(path, "/private/source"); let offset = 0;
    return { size: () => sourceText.length, read: buffer => { reads++; const n = Math.min(buffer.length, sourceText.length - offset); sourceText.copy(buffer, 0, offset, offset+n); offset += n; return n; }, verify: () => {}, close: () => { closed++; } };
  } };
  const f = fixture(t, executor, async r => {
    if (r.method === "transfer.offer") return descriptor("one");
    if (r.method === "transfer.chunk") { chunks.push(Buffer.from(r.params.data,"base64")); return { offset: r.params.offset + Buffer.from(r.params.data,"base64").length }; }
    if (r.method === "transfer.finish") { assert.equal(r.params.sha256, hash); finished.resolve(); return { id: "one", outcome: "completed", bytes: sourceText.length, sha256: hash }; }
    if (r.method === "session.list") return { sessions: [] };
    throw new Error("unexpected method");
  });
  const caller = new Caller(f.client, { transfers: f.transfers });
  await caller.action("transfer.offer", { target: "destination@local", source: "/private/source" });
  assert.equal(opened, 0);
  await f.bus.call("transfer.prepare", { id: "one", role: "source" });
  await f.bus.call("transfer.start", { id: "one", role: "source", size: sourceText.length, credit: 262144 });
  await finished.promise;
  await f.bus.call("transfer.notice", { kind: "terminal", id: "one", outcome: "completed", bytes: sourceText.length, sha256: hash });
  assert.deepEqual(Buffer.concat(chunks), sourceText); assert.equal(reads,1); assert.equal(opened,1); assert.equal(closed,1);
  assert.equal(JSON.stringify(f.incoming).includes("/private/source"), false);
  assert.deepEqual(await caller.list(), { sessions: [] }); assert.equal(f.transfers.pending,0); assert.equal(f.transfers.active,0);
});

test("destination refuses wrong offset/digest without write or publish; one commit and receiver-local projection", { timeout: 2000 }, async t => {
  const captured = []; let commits = 0, aborts = 0;
  const f = fixture(t, { ...noExecutor, openTransferDestination: async () => ({ write: raw => { captured.push(Buffer.from(raw)); return raw.length; }, commit: () => { commits++; return { published: true }; }, abort: () => { aborts++; } }) }, async r => descriptor(r.params.id));
  await incomingDestination(f);
  await assert.rejects(f.bus.call("transfer.chunk", { id: "one", offset: 1, data: sourceText.toString("base64") }), { code: -32603 });
  assert.equal(captured.length,0);
  await f.bus.call("transfer.chunk", { id: "one", offset: 0, data: sourceText.toString("base64") });
  await assert.rejects(f.bus.call("transfer.finish", { id: "one", bytes: sourceText.length, sha256: "0".repeat(64) }), { code: -32603 });
  assert.equal(commits,0);
  const complete = await f.bus.call("transfer.finish", { id: "one", bytes: sourceText.length, sha256: hash });
  await assert.rejects(f.bus.call("transfer.finish", { id: "one", bytes: sourceText.length, sha256: hash }), { code: -32603 });
  const delivery = { message_id: "transfer-notice-one", body: JSON.stringify({ kind: "terminal", ...complete }) };
  const project = f.transfers.projectDelivery(f.client, delivery);
  await project(undefined, delivery); assert.equal(JSON.parse(delivery.body).destination,"/local/destination");
  await f.bus.call("transfer.notice", { kind: "terminal", ...complete });
  assert.equal(commits,1); assert.equal(aborts,1); assert.equal(f.transfers.active,0); assert.deepEqual(Buffer.concat(captured),sourceText);
});

test("unconfirmed publication never aborts/retries or releases accepted charge early", { timeout: 2000 }, async t => {
  let commits = 0, aborts = 0;
  const f = fixture(t, { ...noExecutor, openTransferDestination: async () => ({ write: raw => raw.length, commit: () => { commits++; return { published: false, error: ErrTransferCommitUnknown }; }, abort: () => { aborts++; } }) }, async r => descriptor(r.params.id));
  await incomingDestination(f);
  await f.bus.call("transfer.chunk", { id: "one", offset: 0, data: sourceText.toString("base64") });
  const unknown = await f.bus.call("transfer.finish", { id: "one", bytes: sourceText.length, sha256: hash });
  assert.equal(unknown.outcome,"unknown");
  await assert.rejects(f.bus.call("transfer.notice", { kind: "terminal", ...unknown }), { code: -32603 });
  assert.equal(f.transfers.active,1); assert.equal(aborts,0); assert.equal(commits,1);
  f.scope.abort(); await f.transfers.closeConnectionGeneration(); assert.equal(f.transfers.active,0); assert.equal(aborts,0);
});

test("late confirmed publication upgrades only captured receiver notice off reader", { timeout: 2000 }, async t => {
  const publishing = deferred(), release = deferred();
  const f = fixture(t, { ...noExecutor, openTransferDestination: async () => ({ write: raw => raw.length, commit: async () => { publishing.resolve(); await release.promise; return { published: true }; }, abort: () => {} }) }, async r => descriptor(r.params.id));
  await incomingDestination(f); await f.bus.call("transfer.chunk", { id: "one", offset: 0, data: sourceText.toString("base64") });
  const finish = f.bus.call("transfer.finish", { id: "one", bytes: sourceText.length, sha256: hash }); await publishing.promise;
  const delivery = { message_id: "transfer-notice-one", body: JSON.stringify({ kind:"terminal", id:"one", outcome:"unknown",bytes:sourceText.length,reason:"link_lost" }) };
  const project = f.transfers.projectDelivery(f.client,delivery); let projected = false;
  const projection = project(undefined,delivery).then(() => { projected = true; });
  await Promise.resolve(); assert.equal(projected,false); release.resolve(); await finish; await projection;
  assert.equal(JSON.parse(delivery.body).outcome,"completed"); assert.equal(JSON.parse(delivery.body).sha256,hash);
});

test("blocked prepare cancels outside handle lock and ordinary work is not disconnected", { timeout: 2000 }, async t => {
  const entered = deferred();
  const f = fixture(t, { ...noExecutor, openTransferSource: async signal => { entered.resolve(); await new Promise(resolve => signal.addEventListener("abort",resolve,{once:true})); throw new Error("cancelled"); } }, async r => r.method === "transfer.offer" ? descriptor("one") : {sessions:[]});
  await f.transfers.offer({target:"destination@local",source:"/private/source",name:"file"});
  const preparing = assert.rejects(f.bus.call("transfer.prepare",{id:"one",role:"source"}),{code:-32603}); await entered.promise;
  await f.bus.call("transfer.notice",{kind:"terminal",id:"one",outcome:"cancelled",bytes:0}); await preparing;
  assert.deepEqual(await f.client.call("session.list",{}),{sessions:[]}); assert.equal(f.transfers.active,0);
});

test("legacy daemon and unattached raw caller refuse transfers locally, preserving ordinary actions", { timeout: 2000 }, async t => {
  const f = fixture(t,noExecutor,async r => r.method === "session.hello" ? {} : {sessions:[]});
  await f.transfers.helloObserved(f.client,{session_id:"source@local",product:"test",groups:[],info:{}},f.scope.signal);
  assert.throws(() => f.transfers.offer({target:"destination@local",source:"/private/source",name:"file"}),/unsupported_transfer/);
  assert.equal(f.incoming[0].params.info.sessionbus_transfer,1);
  await assert.rejects(new Caller(f.client).action("transfer.offer",{target:"target",source:"/private/path"}),/unsupported_transfer/);
  assert.deepEqual(await f.client.call("session.list",{}),{sessions:[]});
});

test("default worker bootstrap opts in only with the versioned daemon token", { timeout: 2000 }, async t => {
  for (const token of ["legacy-random", "sbt1:random"]) {
    const [client, server] = pair(), got = deferred(); let bus;
    bus = new Connection(server,false,r => { got.resolve(r.params); void bus.result(r,{}); });
    const worker = new Worker({hello:() => ({product:"fixture",supported_open_fields:[],extra_arguments:[]})},{SESSIONBUS_SOCKET:"fixture",SESSIONBUS_LAUNCH_TOKEN:token},{connect:() => client});
    const serving = worker.serve().catch(error=>error), hello = await got.promise;
    assert.equal(hello.supports_transfer,token.startsWith("sbt1:")?true:undefined);
    worker.shutdown(); bus.close(); await serving;
  }
});

test("source verification failure uses the fixed pre-finish machine failure without cancellation or publication", { timeout: 2000 }, async t => {
 const failed=deferred();let offset=0;
 const f=fixture(t,{...noExecutor,openTransferSource:async()=>({size:()=>sourceText.length,read:buffer=>{sourceText.copy(buffer,0,offset);const n=sourceText.length-offset;offset+=n;return n;},verify:()=>{throw new Error("private native diagnostic must stay local");},close:()=>{}})},async r=>{
  if(r.method==="transfer.offer")return descriptor("one");
  if(r.method==="transfer.chunk")return{offset:sourceText.length};
  if(r.method==="transfer.fail"){assert.deepEqual(r.params,{id:"one",reason:"source_changed"});failed.resolve();return{id:"one",outcome:"failed",bytes:sourceText.length,reason:"source_changed"};}
  throw new Error("unexpected cancellation or publication request");
 });
 await f.transfers.offer({target:"destination@local",source:"/private/source",name:"file"});
 await f.bus.call("transfer.prepare",{id:"one",role:"source"});await f.bus.call("transfer.start",{id:"one",role:"source",size:sourceText.length,credit:262144});await failed.promise;
 assert.equal(f.incoming.some(r=>r.method==="transfer.cancel"||r.method==="transfer.finish"),false);
 assert.equal(JSON.stringify(f.incoming).includes("private native diagnostic"),false);
});

test("live accept keeps local path/overwrite binding and only the receiver chooses overwrite", { timeout: 2000 }, async t => {
  const f = fixture(t, noExecutor, async r => {
    if (r.method === "transfer.offer") return descriptor("one");
    if (r.method === "transfer.accept") return descriptor("two");
    throw new Error("unexpected request");
  });
  await f.transfers.offer({ target: "destination@local", source: "/local/source", name: "file" });
  await assert.rejects(f.transfers.accept({ id: "one", path: "/local/other" }), /binding_conflict/);
  await assert.rejects(f.transfers.accept({ id: "one", path: "/local/source", overwrite: true }), /binding_conflict/);
  await f.bus.call("transfer.notice", { id: "two", kind: "offer", name: "file", from: { session_id: "sender@local", product: "fixture", groups: [] } });
  await f.transfers.accept({ id: "two", path: "/local/destination", overwrite: false });
  await assert.rejects(f.transfers.accept({ id: "two", path: "/local/different" }), /binding_conflict/);
  await assert.rejects(f.transfers.accept({ id: "two", path: "/local/destination", overwrite: true }), /binding_conflict/);
  assert.equal(f.incoming.filter(r => r.method === "transfer.accept").length, 1);
});

test("unsupported JS hard-link publication fails clearly without UNKNOWN", { timeout: 2000 }, async t => {
 const f=fixture(t,{...noExecutor,openTransferDestination:async()=>({write:raw=>raw.length,commit:()=>({published:false,error:Object.assign(new Error("unsupported link"),{code:"ENOTSUP"})}),abort:()=>{}})},async r=>descriptor(r.params.id));
 await incomingDestination(f);
 await f.bus.call("transfer.chunk",{id:"one",offset:0,data:sourceText.toString("base64")});
 const result=await f.bus.call("transfer.finish",{id:"one",bytes:sourceText.length,sha256:hash});
 assert.equal(result.outcome,"failed");assert.equal(result.reason,"atomic_no_replace_unsupported");
 await f.bus.call("transfer.notice",{kind:"terminal",...result});
});

for (const code of ["ENOTSUP", "EOPNOTSUPP"]) {
 test(`unsupported JS overwrite publication reports the attempted operation (${code})`, { timeout: 2000 }, async t => {
  const f=fixture(t,{...noExecutor,openTransferDestination:async(_signal,_path,overwrite)=>{
   assert.equal(overwrite,true);
   return {write:raw=>raw.length,commit:()=>({published:false,error:Object.assign(new Error("unsupported overwrite rename"),{code})}),abort:()=>{}};
  }},async r=>descriptor(r.params.id));
  await f.bus.call("transfer.notice",{kind:"offer",id:"one",name:"file",from:{session_id:"source@local",product:"test",groups:[]}});
  await f.transfers.accept({id:"one",path:"/local/destination",overwrite:true});
  await f.bus.call("transfer.prepare",{id:"one",role:"destination",size:sourceText.length});
  await f.bus.call("transfer.start",{id:"one",role:"destination",size:sourceText.length});
  await f.bus.call("transfer.chunk",{id:"one",offset:0,data:sourceText.toString("base64")});
  const result=await f.bus.call("transfer.finish",{id:"one",bytes:sourceText.length,sha256:hash});
  assert.equal(result.outcome,"failed");assert.equal(result.reason,"publication_failed");
  await f.bus.call("transfer.notice",{kind:"terminal",...result});
 });
}
