import test from "node:test";
import assert from "node:assert/strict";
import { getTokenTopMovers } from "./analytics";
import { databasePool } from "./connection";

test("top mover query excludes non-wallet addresses", async () => {
  const originalQuery = Object.getOwnPropertyDescriptor(databasePool, "query");
  let queryText: string | undefined;
  Object.defineProperty(databasePool, "query", {
    configurable: true,
    value: async (text: string) => {
      queryText = text;
      return { rows: [] };
    },
  });

  try {
    await getTokenTopMovers("KCAL", 7, 10);
  } finally {
    if (originalQuery) {
      Object.defineProperty(databasePool, "query", originalQuery);
    } else {
      Reflect.deleteProperty(databasePool, "query");
    }
  }

  assert.ok(queryText);
  assert.match(queryText, /COALESCE\(l\.address, b\.address\) ~ '\^\[PS\]/);
});
