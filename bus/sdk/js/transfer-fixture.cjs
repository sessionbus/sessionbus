// SPDX-License-Identifier: MIT
// Controlled private-test executor. Never packaged or used as a native grant.
"use strict";
const fs = require("node:fs/promises"), path = require("node:path");
const { connectPeer } = require("./index.js");
const { FilesystemTransferExecutor } = require("./transfer-filesystem.js");
const [socket, role, ownPath, mode] = process.argv.slice(2);
let finish;
const terminal = new Promise(resolve => { finish = resolve; });
const executor = {
 async openTransferSource(signal, source) {
  if (role !== "source" || source !== ownPath || signal.aborted) throw new Error("fixture denies path");
  const file = await fs.open(source,"r"), stat = await file.stat();
  if (!stat.isFile()) { await file.close(); throw new Error("not a regular fixture file"); }
  return { size: () => stat.size, read: async buffer => (await file.read(buffer)).bytesRead, verify: async () => { const after = await file.stat(); if (after.dev!==stat.dev || after.ino!==stat.ino || after.size!==stat.size) throw new Error("fixture source changed"); }, close: () => file.close() };
 },
 async openTransferDestination(signal, destination, overwrite) {
  if (role !== "destination" || destination !== ownPath || overwrite || signal.aborted) throw new Error("fixture denies path");
  const temp = ownPath+".private-temp", file = await fs.open(temp,"wx"); let published = false, closed = false;
  const close = async () => { if (!closed) { closed = true; await file.close(); } };
  return { write: async raw => (await file.write(raw)).bytesWritten, commit: async () => { await close(); try { await fs.link(temp,ownPath); } catch(error) { return {published:false,error}; } published=true; try {await fs.unlink(temp);return{published:true};}catch(error){return{published:true,error};} }, abort: async () => {await close(); if (!published) await fs.unlink(temp).catch(error => {if(error.code!=="ENOENT")throw error;});} };
 }
};
// This mode wraps the real default executor only to move its parent after a
// real streamed write. It does not replace publication or cleanup behavior.
const defaultFS = new FilesystemTransferExecutor(), opened = []; let moved = false;
const originalOpen = fs.open;
if (mode === "parent-rename") fs.open = async (...args) => { const h = await originalOpen(...args); opened.push(h); return h; };
const changingParent = {
 openTransferSource: (...args) => defaultFS.openTransferSource(...args),
 openTransferDestination: async (...args) => {
  const d = await defaultFS.openTransferDestination(...args);
  return { write: async raw => { const n = await d.write(raw); if (!moved) { moved=true; const parent=path.dirname(ownPath); await fs.rename(parent,parent+"-moved"); await fs.mkdir(parent); } return n; }, commit: () => d.commit(), abort: () => d.abort() };
 }
};
const peer = connectPeer({session_id:"js",product:"controlled-js-fixture",groups:["shared"],info:{}},async (_signal,delivery) => {
 let notice;try{notice=JSON.parse(delivery.body);}catch{}
 if(notice?.kind==="terminal")finish(notice);
 return{disposition:"written"};
},{SESSIONBUS_SOCKET:socket},{transferExecutor:mode === "parent-rename" ? changingParent : mode === "default" ? undefined : executor,schedule:()=>{throw new Error("fixture must not reconnect");}});
(async () => {
 await peer.ready;
 if(role==="source") await peer.caller.action("transfer.offer",{target:"go@local",source:ownPath,name:path.basename(ownPath)});
 else await peer.caller.action("transfer.request",{target:"go@local",what:"the private test data",destination:ownPath,name:"fixture"});
 const result=await terminal;
 if(mode === "parent-rename") {
  if(result.outcome!=="failed" || !result.reason.includes("destination_parent_changed") || !result.reason.match(/leftover_temp=\.sessionbus-transfer-[0-9a-f]{32}\.tmp/)) throw new Error("missing truthful parent/cleanup failure");
  if(peer.transfers.active!==0 || peer.transfers.pending!==0 || opened.length!==2) throw new Error("terminal slot not retired");
  for(const h of opened) { let live=false; try { await h.stat();live=true; } catch(error) { if(error.code!=="EBADF")throw error; } if(live)throw new Error("SDK filesystem handle remains live"); }
 } else if(result.outcome!=="completed")throw new Error("transfer did not complete");
 const ordinary=await peer.caller.send({target:"go@local",message:"ordinary authenticated JavaScript continuation"});
 if(ordinary.deliveries.length!==1||ordinary.deliveries[0].disposition!==(mode==="parent-rename"?"injected":"written"))throw new Error("ordinary continuation lost");
 const summary = {role,outcome:result.outcome,bytes:result.bytes,sha256:result.sha256,reason:result.reason,active:peer.transfers.active,pending:peer.transfers.pending,closedHandles:mode==="parent-rename"?opened.length:undefined,ordinaryMessageID:ordinary.message_id};
 if(mode==="parent-rename") { process.stdout.write(JSON.stringify(summary)+"\n"); await new Promise(resolve=>process.stdin.once("data",resolve)); }
 await peer.transfers.closeConnectionGeneration();peer.shutdown();await peer.closed;
 if(mode!=="parent-rename")process.stdout.write(JSON.stringify(summary)+"\n");
})().catch(error=>{peer.shutdown();process.stderr.write(String(error.stack||error)+"\n");process.exitCode=1;});
