// Strict SDK wire control only: no product/native CLI or reconnect.
"use strict";
const assert = require("node:assert/strict");
const net = require("node:net");
const { Connection } = require(process.argv[3]);
const socket = net.createConnection(process.argv[2]);
const connection = new Connection(socket, true);
const controller = new AbortController();
const timer = setTimeout(() => controller.abort(), 3000);
(async () => {
  let refusal;
  try { await connection.call("session.list", {}, controller.signal); }
  catch (error) { refusal = error; }
  assert.equal(refusal?.code, -32603);
  assert.equal(refusal?.message, "internal");
  assert.equal(refusal?.data, "internal");
  const result = await connection.call("session.list", {}, controller.signal);
  assert.deepEqual(result, {sessions: []});
  assert.equal(connection.controller.signal.aborted, false);
  process.stdout.write(JSON.stringify({fallback: refusal.code, continuation: result, sameConnection: true}) + "\n");
})().catch(error => { process.stderr.write(String(error.stack || error) + "\n"); process.exitCode = 1; })
  .finally(() => { clearTimeout(timer); socket.destroy(); });
