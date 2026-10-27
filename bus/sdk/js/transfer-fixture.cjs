// SPDX-License-Identifier: MIT
// Controlled private-test executor. Never packaged or used as a native grant.
"use strict";
const fs = require("node:fs/promises"), path = require("node:path");
const { connectPeer } = require("./index.js");
const [socket, role, ownPath] = process.argv.slice(2);
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
const peer = connectPeer({session_id:"js",product:"controlled-js-fixture",groups:["shared"],info:{}},async (_signal,delivery) => {
 let notice;try{notice=JSON.parse(delivery.body);}catch{}
 if(notice?.kind==="terminal")finish(notice);
 return{disposition:"written"};
},{SESSIONBUS_SOCKET:socket},{transferExecutor:executor,schedule:()=>{throw new Error("fixture must not reconnect");}});
(async () => {
 await peer.ready;
 if(role==="source") await peer.caller.action("transfer.offer",{target:"go@local",source:ownPath,name:path.basename(ownPath)});
 else await peer.caller.action("transfer.request",{target:"go@local",what:"the private test data",destination:ownPath,name:"fixture"});
 const result=await terminal;
 if(result.outcome!=="completed")throw new Error("transfer did not complete");
 const ordinary=await peer.caller.send({target:"go@local",message:"ordinary authenticated JavaScript continuation"});
 if(ordinary.deliveries.length!==1||ordinary.deliveries[0].disposition!=="written")throw new Error("ordinary continuation lost");
 await peer.transfers.closeConnectionGeneration();peer.shutdown();await peer.closed;
 process.stdout.write(JSON.stringify({role,outcome:result.outcome,bytes:result.bytes,sha256:result.sha256})+"\n");
})().catch(error=>{peer.shutdown();process.stderr.write(String(error.stack||error)+"\n");process.exitCode=1;});
