// SPDX-License-Identifier: MIT
"use strict";
const test=require("node:test"),assert=require("node:assert/strict");
const {EventEmitter}=require("node:events");
const {setImmediate:nextTurn}=require("node:timers/promises");
const {Connection}=require("./connection.js");
class HeldStream extends EventEmitter{
 callbacks=[];bodies=[];
 write(body,callback){this.bodies.push(JSON.parse(body));this.callbacks.push(callback);return false;}
 destroy(){this.destroyed=true;}
}
function response(stream,id,result){stream.emit("data",Buffer.from(JSON.stringify({jsonrpc:"2.0",id,result})+"\n"));}

test("bulk has four independent slots, priority-dispatched monotonic IDs and fair stream choice",async()=>{
 const stream=new HeldStream(),c=new Connection(stream,true);
 const calls=[c.call("transfer.chunk",{id:"a",offset:0,data:"YQ=="}),c.call("transfer.chunk",{id:"a",offset:1,data:"Yg=="}),c.call("transfer.chunk",{id:"b",offset:0,data:"YQ=="}),c.call("transfer.chunk",{id:"b",offset:1,data:"Yg=="})];
 const refused=assert.rejects(c.call("transfer.chunk",{id:"c",offset:0,data:"YQ=="}),{code:-32003});
 calls.push(c.call("session.list",{}));
 assert.deepEqual(stream.bodies.map(x=>x.method),["session.list","transfer.chunk"]);
 calls.push(c.call("session.list",{}));
 assert.deepEqual(stream.bodies.map(x=>x.id),[1,2,3]);
 assert.equal(c.signal.aborted,false);assert.equal(c.bulkWrites,4);assert.equal(c.normalPending,2);
 await refused;
 // Release each actual write once. Queued bulk IDs do not exist until selected.
 for(let index=0;index<6;index++){
  assert.ok(stream.callbacks[index]);stream.callbacks[index]();
  const f=stream.bodies[index];response(stream,f.id,f.method==="session.list"?{sessions:[]}:{offset:f.params.offset+1});
 }
 await Promise.all(calls);
 assert.deepEqual(stream.bodies.map(x=>x.id),[1,2,3,4,5,6]);
 assert.deepEqual(stream.bodies.filter(x=>x.method==="transfer.chunk").map(x=>x.params.id),["a","b","a","b"]);
 assert.equal(c.bulkWriteBytes,0);assert.equal(c.bulkWrites,0);assert.equal(c.normalPending,0);assert.equal(c.bulkPending,0);
 c.close();stream.emit("close");
});

test("bulk reply drain suppresses cancelled observer without consuming control capacity",async()=>{
 const stream=new HeldStream(),c=new Connection(stream,true);const abort=new AbortController();let observed=0;
 const call=c.call("transfer.chunk",{id:"a",offset:0,data:"YQ=="},abort.signal,()=>observed++).then(()=>assert.fail("cancelled call succeeded"),error=>error);
 await nextTurn();abort.abort(new Error("abandon"));
 assert.equal(c.bulkPending,1);assert.equal(c.normalPending,0);
 stream.callbacks[0]();assert.match((await call).message,/abandon/);response(stream,stream.bodies[0].id,{offset:1});
 assert.equal(observed,0);assert.equal(c.bulkPending,0);assert.equal(c.signal.aborted,false);
 c.close();stream.emit("close");
});

test("inbound bulk saturation is a bounded refusal and keeps ordinary replies healthy",async()=>{
 const stream=new HeldStream(),requests=[];const c=new Connection(stream,false,r=>requests.push(r));
 for(let id=1;id<=5;id++)stream.emit("data",Buffer.from(JSON.stringify({jsonrpc:"2.0",id,method:"transfer.chunk",params:{id:"live",offset:id-1,data:"YQ=="}})+"\n"));
 assert.equal(requests.length,4);assert.equal(c.inboundBulk,4);assert.equal(stream.bodies[0].error.code,-32003);assert.equal(c.signal.aborted,false);
 stream.callbacks[0]();const done=c.result(requests[0],{offset:1});stream.callbacks[1]();await done;
 assert.equal(c.inboundBulk,3);c.close();stream.emit("close");
});
