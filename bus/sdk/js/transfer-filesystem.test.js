// SPDX-License-Identifier: MIT
"use strict";

const test = require("node:test"), assert = require("node:assert/strict");
const fs = require("node:fs/promises"), path = require("node:path"), os = require("node:os");
const { FilesystemTransferExecutor } = require("./transfer-filesystem.js");
const { Transfers, Worker } = require("./index.js");
async function directory(t) { const dir = await fs.mkdtemp(path.join(os.tmpdir(), "sessionbus-direct-fs-")); t.after(() => fs.rm(dir, { recursive: true, force: true })); return dir; }
async function noTemps(dir) { assert.equal((await fs.readdir(dir)).some(name => name.startsWith(".sessionbus-transfer-")), false); }

test("raw and worker defaults use direct current-user filesystem access", () => {
  assert.ok(new Transfers().executor instanceof FilesystemTransferExecutor);
  assert.ok(new Worker({}).transfers.executor instanceof FilesystemTransferExecutor);
});
test("default source opens normal symlinks, detects same-handle change and preserves real I/O errors", async t => {
  const dir = await directory(t), source = path.join(dir, "source"), link = path.join(dir, "link"), x = new FilesystemTransferExecutor();
  await fs.writeFile(source, "data"); await fs.symlink(source, link);
  const handle = await x.openTransferSource(undefined, link), buffer = Buffer.alloc(4);
  assert.equal(handle.size(), 4); assert.equal(await handle.read(buffer), 4); assert.equal(buffer.toString(), "data"); await handle.verify();
  await fs.writeFile(source, "changed"); await assert.rejects(handle.verify(), /source changed/); await handle.close(); await handle.close();
  await assert.rejects(x.openTransferSource(undefined, dir), /regular file/);
  await assert.rejects(x.openTransferSource(undefined, path.join(dir, "missing")), { code: "ENOENT" });
  const controller = new AbortController(); controller.abort();
  await assert.rejects(x.openTransferSource(controller.signal, source), { name: "AbortError" });
  await assert.rejects(x.openTransferDestination(controller.signal, path.join(dir, "cancelled"), false), { name: "AbortError" });
  await noTemps(dir);
});
test("default destination publishes empty/populated files once and never retracts after cancellation", async t => {
  const dir = await directory(t), x = new FilesystemTransferExecutor();
  for (const [i, text] of ["", "entire content"].entries()) {
    const target = path.join(dir, `target-${i}`), controller = new AbortController();
    const d = await x.openTransferDestination(controller.signal, target, false);
    assert.equal(await d.write(Buffer.from(text)), text.length);
    await assert.rejects(fs.stat(target), { code: "ENOENT" });
    assert.deepEqual(await d.commit(), { published: true }); controller.abort();
    assert.deepEqual(await d.commit(), { published: true }); await d.abort(); await d.abort();
    assert.equal(await fs.readFile(target, "utf8"), text); await noTemps(dir);
  }
});
test("no-overwrite is atomic across existing/racing writers; explicit overwrite replaces whole file", async t => {
  const dir = await directory(t), x = new FilesystemTransferExecutor(), target = path.join(dir, "target");
  await fs.writeFile(target, "original");
  const existing = await x.openTransferDestination(undefined, target, false); await existing.write(Buffer.from("replacement"));
  const refusal = await existing.commit(); assert.equal(refusal.published, false); assert.equal(refusal.error.code, "EEXIST"); await existing.abort();
  assert.equal(await fs.readFile(target, "utf8"), "original");
  const overwrite = await x.openTransferDestination(undefined, target, true); await overwrite.write(Buffer.from("replacement"));
  assert.deepEqual(await overwrite.commit(), { published: true }); await overwrite.abort(); assert.equal(await fs.readFile(target, "utf8"), "replacement");
  const racing = path.join(dir, "race"), writers = await Promise.all([0, 1].map(() => x.openTransferDestination(undefined, racing, false)));
  await Promise.all(writers.map(d => d.write(Buffer.from("whole")))); const results = await Promise.all(writers.map(d => d.commit()));
  assert.equal(results.filter(r => r.published).length, 1); await Promise.all(writers.map(d => d.abort()));
  assert.equal(await fs.readFile(racing, "utf8"), "whole"); await noTemps(dir);
});
test("cancelled unpublished destination cleans one temp and cannot later commit", async t => {
  const dir = await directory(t), x = new FilesystemTransferExecutor(), target = path.join(dir, "target"), controller = new AbortController();
  const d = await x.openTransferDestination(controller.signal, target, false); await d.write(Buffer.from("pending")); controller.abort();
  await assert.rejects(d.write(Buffer.from("late")), { name: "AbortError" }); await d.abort();
  const result = await d.commit(); assert.equal(result.published, false); assert.ok(result.error);
  await assert.rejects(fs.stat(target), { code: "ENOENT" }); await noTemps(dir);
});
test("changed parent does not redirect publication or pretend owned temp cleanup succeeded", async t => {
  const dir = await directory(t), parent = path.join(dir, "parent"), moved = path.join(dir, "moved"), x = new FilesystemTransferExecutor();
  await fs.mkdir(parent); const d = await x.openTransferDestination(undefined, path.join(parent, "target"), false); await d.write(Buffer.from("pending"));
  await fs.rename(parent, moved); await fs.mkdir(parent);
  const result = await d.commit(); assert.equal(result.published, false); assert.match(result.error.message, /parent changed/);
  await assert.rejects(d.abort(), /cleanup failed/); await assert.rejects(fs.stat(path.join(parent, "target")), { code: "ENOENT" });
  assert.equal((await fs.readdir(moved)).length, 1); // Explicit residue, removed only by this private fixture's teardown.
});
