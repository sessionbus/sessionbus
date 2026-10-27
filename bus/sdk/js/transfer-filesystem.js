// SPDX-License-Identifier: MIT
"use strict";

const fs = require("node:fs/promises");
const { constants } = require("node:fs");
const { randomBytes } = require("node:crypto");
const path = require("node:path");

function check(signal) { signal?.throwIfAborted(); }
function sameFile(a, b) { return a.dev === b.dev && a.ino === b.ino; }

// The standard executor uses current-user OS access, without product policy or
// subprocesses. Pending bindings open nothing. Only one sibling temp is written.
class FilesystemTransferExecutor {
  async openTransferSource(signal, source) {
    check(signal);
    const file = await fs.open(source, constants.O_RDONLY | constants.O_NONBLOCK);
    let stat;
    try {
      stat = await file.stat({ bigint: true });
      if (!stat.isFile()) throw new Error("transfer source is not a regular file");
      check(signal);
    } catch (error) { await file.close(); throw error; }
    let closed;
    return {
      size: () => Number(stat.size),
      read: async (buffer, currentSignal = signal) => { check(currentSignal); return (await file.read(buffer)).bytesRead; },
      verify: async () => {
        check(signal);
        const after = await file.stat({ bigint: true });
        if (!sameFile(stat, after) || stat.size !== after.size || stat.mtimeNs !== after.mtimeNs) throw new Error("transfer source changed");
      },
      close: () => closed ??= file.close(),
    };
  }
  async openTransferDestination(signal, destination, overwrite) {
    check(signal);
    if (typeof destination !== "string" || !destination || destination.endsWith(path.sep) || [".", ".."].includes(path.basename(destination))) throw new Error("invalid transfer destination");
    // Resolve symlinked parents once. Public Node fs has no descriptor-relative
    // link/rename; a later parent replacement is an explicit I/O/cleanup failure.
    const directory = await fs.realpath(path.dirname(destination));
    const parent = await fs.open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
    let parentStat, file, temp;
    const target = path.join(directory, path.basename(destination));
    try {
      parentStat = await parent.stat({ bigint: true });
      temp = path.join(directory, `.sessionbus-transfer-${randomBytes(16).toString("hex")}.tmp`);
      file = await fs.open(temp, "wx", 0o600);
      check(signal);
    } catch (error) {
      const errors = [error];
      if (file) {
        try { await file.close(); } catch (failure) { errors.push(failure); }
        try { await fs.unlink(temp); } catch (failure) { errors.push(failure); }
      }
      try { await parent.close(); } catch (failure) { errors.push(failure); }
      throw errors.length === 1 ? error : new AggregateError(errors, "transfer prepare cleanup failed");
    }
    let tail = Promise.resolve(), attempted = false, aborted = false, published = false, result;
    const serial = callback => { const next = tail.then(callback); tail = next.catch(() => {}); return next; };
    const closeFile = async () => { if (file) { const owned = file; file = null; await owned.close(); } };
    let parentClosed = false;
    const closeParent = async () => { if (!parentClosed) { parentClosed = true; await parent.close(); } };
    const checkParent = async () => {
      if (!sameFile(parentStat, await fs.stat(directory, { bigint: true }))) throw new Error("transfer destination parent changed");
    };
    return {
      write: (buffer, currentSignal = signal) => serial(async () => {
        check(currentSignal);
        if (attempted || aborted) throw new Error("transfer destination is closed");
        return (await file.write(buffer)).bytesWritten;
      }),
      commit: () => serial(async () => {
        if (attempted) return result;
        attempted = true;
        result = { published: false };
        try {
          if (aborted) throw new Error("transfer destination is closed");
          await closeFile(); await checkParent();
          if (overwrite) { await fs.rename(temp, target); temp = null; }
          else await fs.link(temp, target);
          published = true; result.published = true;
          if (temp) { await fs.unlink(temp); temp = null; }
          await closeParent();
        } catch (error) { result.error = error; }
        return result;
      }),
      abort: () => serial(async () => {
        aborted = true;
        const errors = [];
        try { await closeFile(); } catch (error) { errors.push(error); }
        if (temp) {
          try { await checkParent(); await fs.unlink(temp); temp = null; }
          catch (error) { errors.push(error); }
        }
        try { await closeParent(); } catch (error) { errors.push(error); }
        if (errors.length) throw new AggregateError(errors, published ? "committed transfer temp cleanup failed" : "transfer temp cleanup failed");
      }),
    };
  }
}

module.exports = { FilesystemTransferExecutor };
