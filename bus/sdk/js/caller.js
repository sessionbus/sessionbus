// SPDX-License-Identifier: MIT

"use strict";

const ACTIONS = Object.freeze(["list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack", "transfer.offer", "transfer.request", "transfer.accept", "transfer.cancel", "transfer.status"]);
const ACTION_METHODS = Object.freeze({ list: "session.list", send: "message.send", spawn: "lane.spawn", describe: "lane.describe", trace: "trace.configure", run: "turn.run", start: "turn.start", status: "turn.status", wait: "turn.wait", interrupt: "turn.interrupt", close: "session.close", forget: "session.close" });

// Run references and results belong to the resident worker, not this caller.
class Caller {
  constructor(connection, options = {}) { this.connection = connection; this.transfers = options.transfers; }
  /**
   * @param {import("./protocol").SessionListRequest} [request]
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").SessionListResult>}
   */
  list(request = {}, cancel) { return this.connection.call("session.list", request, cancel); }
  /**
   * @param {import("./protocol").MessageSendRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").MessageSendResult>}
   */
  send(request, cancel) { return this.connection.call("message.send", request, cancel); }
  /**
   * @param {import("./protocol").LaneDescribeRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").LaneDescribeResult>}
   */
  describe(request, cancel) { return this.connection.call("lane.describe", request, cancel); }
  /**
   * @param {import("./protocol").LaneSpawnRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").LaneSpawnResult>}
   */
  spawn(request, cancel) { return this.connection.call("lane.spawn", request, cancel); }
  /**
   * @param {import("./protocol").TraceConfigureRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").TraceConfigureResult>}
   */
  trace(request, cancel) { return this.connection.call("trace.configure", request, cancel); }
  /**
   * @param {string} sessionID
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").LaneSpawnResult>}
   */
  resume(sessionID, cancel) { return this.spawn({ resume_session_id: sessionID }, cancel); }
  /**
   * @param {import("./protocol").TurnRunRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").RunStatus>}
   */
  run(request, cancel) { return this.connection.call("turn.run", request, cancel); }
  /**
   * @param {import("./protocol").TurnRunRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").RunRef>}
   */
  start(request, cancel) { return this.connection.call("turn.start", request, cancel); }
  /**
   * @param {import("./protocol").ReadRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").RunStatus>}
   */
  status(request, cancel) { return this.connection.call("turn.status", request, cancel); }
  /**
   * @param {import("./protocol").WaitRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<import("./protocol").RunStatus>}
   */
  wait(request, cancel) { return this.connection.call("turn.wait", request, cancel); }
  /**
   * @param {import("./protocol").SessionTarget} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<Record<string, never>>}
   */
  interrupt(request, cancel) { return this.connection.call("turn.interrupt", request, cancel); }
  /**
   * @param {import("./protocol").SessionCloseRequest} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<Record<string, never>>}
   */
  close(request, cancel) { return this.connection.call("session.close", request, cancel); }
  // Once submitted, acknowledgement settles consumption despite later abort.
  /**
   * @param {import("./protocol").RunRef} request
   * @param {AbortSignal} [cancel]
   * @returns {Promise<Record<string, never>>}
   */
  ack(request, cancel) {
    if (cancel?.aborted) return Promise.reject(cancel.reason || new Error("aborted"));
    return this.connection.call("turn.ack", request);
  }
  action(name, args = {}, cancel) {
    if (name.startsWith("transfer.")) {
      const transfers = this.transfers || this.connection.transfers;
      if (!transfers) return Promise.reject(new Error("unsupported_transfer"));
      try { return Promise.resolve(transfers.action(name, args, cancel)); } catch (error) { return Promise.reject(error); }
    }
    if (name === "ack") return this.ack(args, cancel);
    const method = ACTION_METHODS[name];
    if (method) return this.connection.call(method, name === "forget" ? { ...args, forget: true } : args, cancel);
    return Promise.reject(new Error("unknown action"));
  }
  disconnected() {}
}

module.exports = { ACTIONS, Caller };
