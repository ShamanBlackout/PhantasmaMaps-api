import test from "node:test";
import assert from "node:assert/strict";
import { databasePool } from "./connection";
import { getTransactionsPage } from "./listQueries";

test("an empty transaction page preserves the filtered group count", async (t) => {
  const query = t.mock.method(databasePool, "query");
  query.mock.mockImplementationOnce(async () => ({
    command: "SELECT", oid: 0, rowCount: 0, fields: [], rows: [],
  }), 0);
  query.mock.mockImplementationOnce(async () => ({
    command: "SELECT", oid: 0, rowCount: 1, fields: [], rows: [{ total: "17" }],
  }), 1);
  const result = await getTransactionsPage({
    tokenSymbol: "SOUL",
    page: 20,
    pageSize: 2,
  });
  assert.deepEqual(result.items, []);
  assert.equal(result.total, 17);
  assert.equal(query.mock.calls.length, 2);
  assert.deepEqual(query.mock.calls[1].arguments[1], ["SOUL"]);
});
