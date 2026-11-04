// SPDX-License-Identifier: MIT
"use strict";

const CAPABILITY = "transfer/1", TOKEN_PREFIX = "sbt1:";
const CHUNK_BYTES = 65536, CREDIT_BYTES = 262144, FRAME_BYTES = 90112, METADATA_BYTES = 8192;
const MAX_PENDING = 8, MAX_ACTIVE = 2, MAX_BYTES = 1 << 30;
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// This validation never decodes a chunk. Decoding/allocation belongs to the
// endpoint's already reserved transfer stage, not the shared socket reader.
function transferValueValid(method, value, params = true) {
  if (!method.startsWith("transfer.")) return true;
  const chunk = params && method === "transfer.chunk";
  if (Buffer.byteLength(JSON.stringify(value)) > (chunk ? FRAME_BYTES : METADATA_BYTES) - 128) return false;
  if (typeof value?.name === "string" && Buffer.byteLength(value.name) > 255) return false;
  if (typeof value?.what === "string" && Buffer.byteLength(value.what) > 2048) return false;
  if (!chunk) return true;
  const s = value?.data;
  if (typeof s !== "string" || !s.length || s.length % 4) return false;
  const padding = s.endsWith("==") ? 2 : s.endsWith("=") ? 1 : 0;
  if (s.length / 4 * 3 - padding > CHUNK_BYTES) return false;
  let last = 0;
  for (let i = 0; i < s.length - padding; i++) { last = alphabet.indexOf(s[i]); if (last < 0) return false; }
  return !(padding === 1 && (last & 3) || padding === 2 && (last & 15));
}
function transferFrameFits(method, bytes, params = true) {
  return bytes <= (method.startsWith("transfer.") ? params && method === "transfer.chunk" ? FRAME_BYTES : METADATA_BYTES : 1 << 20);
}
module.exports = { MAX_PENDING, MAX_ACTIVE, MAX_BYTES, CAPABILITY, TOKEN_PREFIX, CHUNK_BYTES, CREDIT_BYTES, FRAME_BYTES, METADATA_BYTES, transferValueValid, transferFrameFits };
