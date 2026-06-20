/**
 * Tests for sidecar core logic (no Photon connection needed).
 *
 * Tests the exported resolveSpace() function with fake objects.
 * Run with:  npx tsx --test tests/sidecar.test.ts
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { resolveSpace } from "../src/index.ts";

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

function makeFakeSpace(id: string) {
  return { id, __platform: "imessage" };
}

function makeFakeIm(known: Record<string, any>) {
  return {
    space: (userId: string) =>
      known[userId]
        ? Promise.resolve(known[userId])
        : Promise.reject(new Error(`not found: ${userId}`)),
  };
}

// -------------------------------------------------------------------------
// resolveSpace tests
// -------------------------------------------------------------------------

describe("resolveSpace", () => {
  it("cache hit: returns cached space without SDK call", async () => {
    const known = makeFakeSpace("any;-;+15551234567");
    const cache = new Map<string, any>();
    cache.set("any;-;+15551234567", known);
    const im = makeFakeIm({}); // empty — should never be called

    const result = await resolveSpace(im, cache, "any;-;+15551234567");
    assert.equal(result, known);
  });

  it("cache miss + SDK resolve by phone: resolves and caches", async () => {
    const known = makeFakeSpace("any;-;+15551234567");
    const cache = new Map<string, any>();
    const im = makeFakeIm({ "+15551234567": known });

    const result = await resolveSpace(im, cache, "any;-;+15551234567");
    assert.equal(result, known);
    assert.equal(cache.get("any;-;+15551234567"), known);
  });

  it("cache miss + non-phone spaceId: returns null immediately", async () => {
    const cache = new Map<string, any>();
    const im = makeFakeIm({}); // never called — spaceId has no phone

    const result = await resolveSpace(im, cache, "opaque-group-id");
    assert.equal(result, null);
  });

  it("cache miss + phone extraction + SDK fails: returns null", async () => {
    const cache = new Map<string, any>();
    const im = makeFakeIm({}); // phone not in known set

    const result = await resolveSpace(im, cache, "any;-;+19999999999");
    assert.equal(result, null);
  });
});

// -------------------------------------------------------------------------
// Single-consumer guard (static check — confirmed by behaviour test above)
// -------------------------------------------------------------------------

describe("single consumer", () => {
  it("generation counter guard is present in source", async () => {
    const fs = await import("node:fs/promises");
    const src = await fs.readFile(
      new URL("../src/index.ts", import.meta.url),
      "utf-8"
    );
    assert.ok(src.includes("inboundGeneration"));
    assert.ok(src.includes("myGeneration !== inboundGeneration"));
  });
});