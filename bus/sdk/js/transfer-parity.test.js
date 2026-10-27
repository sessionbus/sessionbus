// SPDX-License-Identifier: MIT
"use strict";
const test = require("node:test"), assert = require("node:assert/strict");
const fs = require("node:fs"), path = require("node:path");
const p = require("./transfer-protocol.js");
test("transfer machine token and numeric SDK bounds match Go", () => {
 const go = fs.readFileSync(path.join(__dirname,"../go/protocol/transfer.go"),"utf8");
 for (const [js,name] of Object.entries({MAX_PENDING:"MaxPendingTransfersPerConnection",MAX_ACTIVE:"MaxActiveTransfersPerConnection",MAX_BYTES:"MaxTransferBytes",CHUNK_BYTES:"TransferChunkBytes",CREDIT_BYTES:"TransferCreditBytes",FRAME_BYTES:"MaxTransferFrameBytes",METADATA_BYTES:"MaxTransferMetadataBytes"})) {
  const match = go.match(new RegExp("\\b"+name+"\\s+(?:int64\\s+)?=\\s*(\\d+)(?:\\s*<<\\s*(\\d+))?"));
  assert.ok(match,name); assert.equal(p[js],Number(match[1])*2**Number(match[2]||0),name);
 }
 assert.match(go,new RegExp('TransferTokenPrefix\\s*=\\s*"'+p.TOKEN_PREFIX+'"'));
 assert.match(go,new RegExp('TransferCapability\\s*=\\s*"'+p.CAPABILITY+'"'));
});
