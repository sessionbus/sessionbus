// SPDX-License-Identifier: MIT

"use strict";

const { isUtf8 } = require("node:buffer");
const METHODS = Object.freeze({
  "transfer.offer": ["TransferOffer", "TransferDescriptor", "client"],
  "transfer.request": ["TransferRequest", "TransferDescriptor", "client"],
  "transfer.accept": ["TransferAccept", "TransferDescriptor", "client"],
  "transfer.status": ["TransferTarget", "TransferDescriptor", "client"],
  "transfer.cancel": ["TransferTarget", "TransferTerminal", "client"],
  "transfer.fail": ["TransferFailure", "TransferTerminal", "client"],
  "transfer.prepare": ["TransferPrepare", "TransferPrepared", "daemon"],
  "transfer.start": ["TransferStart", "TransferStarted", "daemon"],
  "transfer.chunk": ["TransferChunk", "TransferCredit", "both"],
  "transfer.finish": ["TransferFinish", "TransferTerminal", "both"],
  "transfer.notice": ["TransferNotice", "SessionCloseResult", "daemon"],
  "session.hello": ["SessionHelloRequest", "SessionHelloResult", "client"], "session.superseded": ["SessionSupersededRequest", "SessionSupersededResult", "daemon"],
  "session.list": ["SessionListRequest", "SessionListResult", "client"], "message.send": ["MessageSendRequest", "MessageSendResult", "client"],
  "message.deliver": ["MessageDeliverRequest", "MessageDeliverResult", "daemon"], "lane.describe": ["LaneDescribeRequest", "LaneDescribeResult", "client"],
  "lane.spawn": ["LaneSpawnRequest", "LaneSpawnResult", "client"], "trace.configure": ["TraceConfigureRequest", "TraceConfigureResult", "client"], "session.open": ["SessionOpenRequest", "SessionOpenResult", "daemon"],
  "turn.run": ["TurnRunRequest", "RunStatus", "client"],
  "turn.start": ["TurnRunRequest", "RunRef", "client"], "turn.execute": ["ExecuteRequest", "RunRef", "daemon"],
  "turn.status": ["ReadRequest", "RunStatus", "both"], "turn.wait": ["WaitRequest", "RunStatus", "both"],
  "turn.ack": ["RunRef", "SessionCloseResult", "both"], "turn.ready": ["TurnReady", "SessionCloseResult", "client"], "turn.interrupt": ["TurnInterruptRequest", "TurnInterruptResult", "both"], "session.close": ["SessionCloseRequest", "SessionCloseResult", "both"],
});
const MESSAGES = Object.freeze({
  "-32600": "invalid_frame", "-32602": "invalid_hello", "-32603": "internal", "-32001": "unknown_session", "-32002": "not_connected", "-32003": "busy", "-32004": "not_running", "-32005": "already_connected", "-32007": "unknown_product", "-32008": "unsupported_open_field", "-32009": "spawn_failed", "-32010": "timeout", "-32011": "not_committed", "-32012": "superseded", "-32013": "name_taken", "-32014": "unknown_host", "-32015": "forward_lost", "-32016": "unsupported_trace",
});
const { encode, validate } = require("./schema.js");
const { transferValueValid, transferFrameFits } = require("./transfer-protocol.js");
const MAX_WRITES = 256, MAX_WRITE_BYTES = 32 * (1 << 20);

class ProtocolError extends Error {
  constructor(value) { super(value.message); this.code = value.code; this.data = value.data; }
}

class Connection {
  constructor(stream, client, handler = () => {}) {
    this.stream = stream; this.client = client; this.handler = handler; this.buffer = Buffer.alloc(0); this.pending = new Map(); this.next = 0; this.last = 0; this.controller = new AbortController();
    this.writes = new Map(); this.writeBytes = 0;
 this.allPending=new Set();this.bulkPending=0;this.normalPending=0;this.inboundBulk=0;
 this.controlQueue=[];this.bulkQueue=[];this.controlBurst=0;this.bulkInWrite=false;
 this.controlWrites=0;this.bulkWrites=0;this.controlWriteBytes=0;this.bulkWriteBytes=0;
    this.done = new Promise((resolve) => { this.finish = resolve; });
    stream.on("data", (chunk) => this._data(chunk)); stream.on("error", (error) => this.close(error));
    stream.on("close", () => {
      this.close();
      // Some streams omit buffered write callbacks on destruction. Actual
      // close ends their ownership; destroy() alone is not that boundary.
      for (const complete of this.writes.values()) complete(this.signal.reason);
 this.controlQueue=[];this.bulkQueue=[];
    });
  }
  get signal() { return this.controller.signal; }
  async call(method, params, signal, observed) {
    if(!transferValueValid(method,params))throw new Error("invalid transfer value");
    const spec=METHODS[method];if(!spec)throw new Error("invalid method");if(signal?.aborted)throw signal.reason||new Error("aborted");if(this.signal.aborted)throw this.signal.reason;
    const bulk=method==="transfer.chunk";
    if(bulk?this.bulkPending>=4:this.normalPending>=256)throw new ProtocolError({code:-32003,message:"busy"});
    const snapshot=JSON.parse(encode(spec[0],params));
    let accept,reject;const result=new Promise((yes,no)=>{accept=yes;reject=no});
    const pending={method,accept,reject,observed,bulk,id:0,drain:false};this.allPending.add(pending);if(bulk)this.bulkPending++;else this.normalPending++;
    const response=result.then(value=>[true,value],error=>[false,error]);
    const abort=()=>{if(this.allPending.has(pending)){pending.drain=true;pending.observed=undefined;reject(signal.reason||new Error("aborted"));}};signal?.addEventListener("abort",abort,{once:true});
    try{await this._send({jsonrpc:"2.0",method,params:snapshot},pending);const [ok,value]=await response;if(!ok)throw value;return value;}
    catch(error){if(!pending.drain)this._dropPending(pending);throw error;}
    finally{signal?.removeEventListener("abort",abort);}
  }
  _dropPending(pending){
    if(!this.allPending.delete(pending))return;
    if(pending.id)this.pending.delete(pending.id);if(pending.bulk)this.bulkPending--;else this.normalPending--;
  }

  result(request, value) { if (!transferValueValid(request.method, value, false)) return Promise.reject(new Error("invalid transfer result")); const sent = this._send({ jsonrpc: "2.0", id: request.id, result: JSON.parse(encode(METHODS[request.method][1], value)) }); return request._releaseBulk ? sent.finally(request._releaseBulk) : sent; }
  error(request, code, data) { const value = { code, message: MESSAGES[code], ...(data === undefined ? {} : { data }) }; encode("RPCError", value); const sent = this._send({ jsonrpc: "2.0", id: request.id, error: value }); return request._releaseBulk ? sent.finally(request._releaseBulk) : sent; }
  close(cause = new Error("sessionbus connection closed")) {
    if (this.controller.signal.aborted) return; this.controller.abort(cause); this.stream.destroy?.();
    for(const pending of this.allPending)pending.reject(cause);this.allPending.clear();this.pending.clear();this.normalPending=0;this.bulkPending=0;this.finish(cause);
  }
  _send(frame,pending) {
    if(this.signal.aborted)return Promise.reject(this.signal.reason);
    const bulk=frame.method==="transfer.chunk";
    const bound=Buffer.byteLength(JSON.stringify({...frame,...(pending?{id:Number.MAX_SAFE_INTEGER}:{})})+"\n");
    if(!transferFrameFits(frame.method||"",bound))return Promise.reject(new Error("frame too large"));
    if(bulk?this.bulkWrites>=4||this.bulkWriteBytes+bound>4*(88<<10):this.controlWrites>=MAX_WRITES||this.controlWriteBytes+bound>MAX_WRITE_BYTES)return Promise.reject(new ProtocolError({code:-32003,message:"busy"}));
    let resolve,reject;const writing=new Promise((yes,no)=>{resolve=yes;reject=no});let settled=false;
    const job={frame,pending,bulk,stream:bulk?frame.params.id:"",bound,complete:null};
    const complete=(error,transport=true)=>{
      if(settled)return;settled=true;this.writes.delete(writing);this.writeBytes-=bound;
      if(bulk){this.bulkWrites--;this.bulkWriteBytes-=bound;if(job.dispatched)this.bulkInWrite=false;}else{this.controlWrites--;this.controlWriteBytes-=bound;}
      if(error){if(transport)this.close(error);reject(error);}else resolve();
      this._pump();
    };
    job.complete=complete;this.writeBytes+=bound;this.writes.set(writing,complete);
    if(bulk){this.bulkWrites++;this.bulkWriteBytes+=bound;this.bulkQueue.push(job);queueMicrotask(()=>this._pump());}
    else{this.controlWrites++;this.controlWriteBytes+=bound;this.controlQueue.push(job);this._pump();}
    return writing;
  }
  _pump(){
    if(this.pumping||this.signal.aborted)return;this.pumping=true;
    try{while(!this.signal.aborted){
      let job;
      const eligibleBulk=this.bulkQueue.length&&!this.bulkInWrite;
      if(this.controlQueue.length&&(this.controlBurst<8||!eligibleBulk)){job=this.controlQueue.shift();this.controlBurst++;}
      else if(eligibleBulk){let index=this.bulkQueue.findIndex(candidate=>candidate.stream!==this.lastBulkStream);if(index<0)index=0;job=this.bulkQueue.splice(index,1)[0];this.lastBulkStream=job.stream;this.controlBurst=0;this.bulkInWrite=true;}
      else break;
      job.dispatched=true;
      let body;
      try{
        if(job.pending){if(this.next>=Number.MAX_SAFE_INTEGER)throw new Error("request id space exhausted");job.pending.id=++this.next;job.frame.id=job.pending.id;this.pending.set(job.pending.id,job.pending);}
        body=JSON.stringify(job.frame)+"\n";
        if(Buffer.byteLength(body)>job.bound)throw new Error("frame reservation exceeded");
      }catch(error){job.complete(error,false);continue;}
      try{this.stream.write(body,error=>job.complete(error));}catch(error){job.complete(error);}
    }}finally{this.pumping=false;}
  }

  _data(chunk) {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    for (let newline; (newline = this.buffer.indexOf(10)) >= 0;) { const line = this.buffer.subarray(0, newline); this.buffer = this.buffer.subarray(newline + 1); if (line.length + 1 > 1 << 20 || !isUtf8(line) || !this._receive(line.toString())) return this.close(); if (this.rejected) return; }
    if (this.buffer.length + 1 > 1 << 20) this.close();
  }
  _receive(line) {
    let frame; try { frame = JSON.parse(line); } catch { return false; } const request = frame && (Object.hasOwn(frame, "method") || Object.hasOwn(frame, "params"));
    if (!frame || frame.jsonrpc !== "2.0" || !Number.isSafeInteger(frame.id) || frame.id < 1 || !exact(frame, request ? ["jsonrpc", "id", "method", "params"] : Object.hasOwn(frame, "result") ? ["jsonrpc", "id", "result"] : ["jsonrpc", "id", "error"])) return this._reject(frame, request);
    if (request) {
      const spec = METHODS[frame.method], allowed = spec && (spec[2] === "both" || spec[2] === (this.client ? "daemon" : "client"));
      if (frame.id <= this.last || !allowed || !validate(spec[0], frame.params) || !transferValueValid(frame.method, frame.params) || !transferFrameFits(frame.method, Buffer.byteLength(line) + 1)) return this._reject(frame, true); this.last=frame.id;
      const incoming={id:frame.id,method:frame.method,params:frame.params};
      if(frame.method==="transfer.chunk"){
        if(this.inboundBulk>=4){void this.error(incoming,-32003).catch(()=>this.close());return true;}
        this.inboundBulk++;let released=false;incoming._releaseBulk=()=>{if(!released){released=true;this.inboundBulk--;}};
      }
      this.handler(incoming);return true;
    }
    const pending = this.pending.get(frame.id); if (!pending) return false; this._dropPending(pending);
    if (!(frame.error ? validate("RPCError", frame.error) : validate(METHODS[pending.method][1], frame.result) && transferValueValid(pending.method, frame.result, false) && transferFrameFits(pending.method, Buffer.byteLength(line)+1, false))) { pending.reject(new Error("invalid frame")); return false; }
    if (pending.drain) return true;
    try { if (!frame.error) pending.observed?.(frame.result); } catch (error) { pending.reject(error); return false; }
    (frame.error ? pending.reject : pending.accept)(frame.error ? new ProtocolError(frame.error) : frame.result);
    return true;
  }
  _reject(frame, request) {
    if (!request || !Number.isSafeInteger(frame?.id) || frame.id < 1) return false; const method = typeof frame.method === "string" ? frame.method : "";
    this.rejected = true; void this.error({ id: frame.id, method }, method === "session.hello" ? -32602 : -32600).finally(() => this.close()); return true;
  }
}

function exact(value, keys) { return value && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key)); }

module.exports = { Connection, METHODS, ProtocolError };
