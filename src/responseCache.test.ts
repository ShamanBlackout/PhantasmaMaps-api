import test from "node:test";
import assert from "node:assert/strict";
import { SimpleCache } from "./responseCache";

test("SimpleCache evicts the oldest entry when full", () => {
  const cache = new SimpleCache<string>(2);
  cache.set("a", "1", 60_000);
  cache.set("b", "2", 60_000);
  cache.set("c", "3", 60_000);

  assert.equal(cache.size(), 2);
  assert.equal(cache.get("a"), null);
  assert.equal(cache.get("b"), "2");
  assert.equal(cache.get("c"), "3");
});

test("SimpleCache drops expired entries before evicting live ones", () => {
  const cache = new SimpleCache<string>(2);
  cache.set("live", "1", 60_000);
  cache.set("expired", "2", -1);
  cache.set("new", "3", 60_000);

  assert.equal(cache.size(), 2);
  assert.equal(cache.get("live"), "1");
  assert.equal(cache.get("new"), "3");
});

test("SimpleCache rewriting a key refreshes its eviction position", () => {
  const cache = new SimpleCache<string>(2);
  cache.set("a", "1", 60_000);
  cache.set("b", "2", 60_000);
  cache.set("a", "1b", 60_000);
  cache.set("c", "3", 60_000);

  assert.equal(cache.get("a"), "1b");
  assert.equal(cache.get("b"), null);
});
