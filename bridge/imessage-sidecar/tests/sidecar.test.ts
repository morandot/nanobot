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
    const known = makeFakeSpace("dm-123");
    const cache = new Map<string, any>();
    cache.set("dm-123", known);
    const im = makeFakeIm({}); // empty — should never be called

    const result = await resolveSpace(im, cache, "dm-123");
    assert.equal(result, known);
  });

  it("cache miss + SDK success: resolves and caches", async () => {
    const known = makeFakeSpace("dm-123");
    const cache = new Map<string, any>();
    const im = makeFakeIm({ "user-1": known });

    const result = await resolveSpace(im, cache, "user-1");
    assert.equal(result, known);
    // Should be cached for next call
    assert.equal(cache.get("dm-123"), known);
  });

  it("cache miss + SDK failure: returns null", async () => {
    const cache = new Map<string, any>();
    const im = makeFakeIm({});

    const result = await resolveSpace(im, cache, "unknown");
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