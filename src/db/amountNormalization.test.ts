import test from "node:test";
import assert from "node:assert/strict";
import { normalizedAmountSqlExpression } from "./amountNormalization";

test("normalization SQL scales raw amounts by an exact decimal power", () => {
  assert.equal(
    normalizedAmountSqlExpression("amount", "decimals"),
    "amount * POWER(10::numeric, -decimals)",
  );
});
