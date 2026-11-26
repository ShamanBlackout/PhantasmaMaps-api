import test from "node:test";
import assert from "node:assert/strict";
import { types } from "pg";
import { buildSslConfig } from "./connection";

test("UTC timestamp columns retain their instant on a non-UTC host", () => {
  const previousTimezone = process.env.TZ;
  process.env.TZ = "America/Chicago";
  try {
    const parse = types.getTypeParser(types.builtins.TIMESTAMP);
    for (const value of ["2026-01-15 01:02:03.456", "2026-07-15 01:02:03.456"]) {
      const date: unknown = parse(value);
      assert.ok(date instanceof Date);
      assert.equal(date.toISOString(), value.replace(" ", "T") + "Z");
    }
    const zoned: unknown = types.getTypeParser(types.builtins.TIMESTAMPTZ)(
      "2026-01-15 01:02:03-06",
    );
    assert.ok(zoned instanceof Date);
    assert.equal(zoned.toISOString(), "2026-01-15T07:02:03.000Z");
  } finally {
    if (previousTimezone === undefined) delete process.env.TZ;
    else process.env.TZ = previousTimezone;
  }
});

test("database TLS verifies certificates when no custom CA is configured", () => {
  assert.deepEqual(
    buildSslConfig({
      ssl: true,
    }),
    { rejectUnauthorized: true },
  );
});

test("database TLS verifies certificates against a configured CA", () => {
  assert.deepEqual(
    buildSslConfig({
      ssl: true,
      sslCaCert: "test CA",
    }),
    { ca: "test CA", rejectUnauthorized: true },
  );
});

test("database TLS remains disabled when PGSSL is false", () => {
  assert.equal(buildSslConfig({ ssl: false }), undefined);
});
