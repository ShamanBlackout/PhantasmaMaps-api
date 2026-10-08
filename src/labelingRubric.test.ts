import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_RUBRIC_GUARDS,
  LabelingMetrics,
  classifyCandidate,
  computeLabelConfidence,
  isPathTerminalLabel,
  toDisplayLabel,
  toLabelType,
} from "./labelingRubric";

function metrics(overrides: Partial<LabelingMetrics> = {}): LabelingMetrics {
  return {
    inTxCount: 40,
    outTxCount: 40,
    inUniqueCounterparties: 12,
    outUniqueCounterparties: 12,
    inVolume: 1000,
    outVolume: 900,
    inPercentRank: 0.995,
    outPercentRank: 0.995,
    inZScoreLog: 2.5,
    outZScoreLog: 2.5,
    inMadScoreLog: 3.5,
    outMadScoreLog: 3.5,
    highInbound: true,
    highOutbound: true,
    highInCounterparties: true,
    highOutCounterparties: true,
    ...overrides,
  };
}

test("classifies balanced, widely connected wallets as hubs", () => {
  assert.equal(classifyCandidate(metrics(), 500), "hub");
});

test("falls back to directional labels when hub conditions are not met", () => {
  assert.equal(
    classifyCandidate(metrics({ inVolume: 100, outVolume: 1000 }), 500),
    "distributor",
  );
  assert.equal(
    classifyCandidate(
      metrics({ highOutbound: false, highOutCounterparties: false }),
      500,
    ),
    "hub_receiver",
  );
  assert.equal(
    classifyCandidate(
      metrics({
        highOutbound: false,
        highInCounterparties: false,
        highOutCounterparties: false,
      }),
      500,
    ),
    "high_inbound_activity",
  );
  assert.equal(
    classifyCandidate(
      metrics({
        highInbound: false,
        highInCounterparties: false,
        highOutCounterparties: false,
      }),
      500,
    ),
    "high_outbound_activity",
  );
});

test("does not label wallets in tokens below the minimum population", () => {
  assert.equal(
    classifyCandidate(metrics(), DEFAULT_RUBRIC_GUARDS.minPopulation - 1),
    "normal",
  );
});

test("ignores percentile flags that are not backed by real activity", () => {
  // In skewed distributions p95 can be a single transaction; the flag alone must not label.
  const lowActivity = metrics({
    inTxCount: 0,
    outTxCount: 1,
    inUniqueCounterparties: 0,
    outUniqueCounterparties: 1,
  });
  assert.equal(classifyCandidate(lowActivity, 1800), "normal");
});

test("respects custom guards", () => {
  const lowActivity = metrics({ inTxCount: 2, outTxCount: 2 });
  assert.equal(classifyCandidate(lowActivity, 500), "normal");
  assert.equal(
    classifyCandidate(lowActivity, 500, {
      minPopulation: 1,
      minDirectionalTx: 1,
      minCounterparties: 1,
    }),
    "hub",
  );
});

test("confidence rewards strong signals and is discounted for small populations", () => {
  const large = computeLabelConfidence("hub", metrics(), 1000);
  const medium = computeLabelConfidence("hub", metrics(), 50);
  const small = computeLabelConfidence("hub", metrics(), 15);

  assert.equal(large, 0.99);
  assert.ok(medium < large);
  assert.ok(small < medium);

  const weak = computeLabelConfidence(
    "high_inbound_activity",
    metrics({
      inZScoreLog: 0,
      outZScoreLog: 0,
      inMadScoreLog: 0,
      outMadScoreLog: 0,
      inPercentRank: 0.9,
      outPercentRank: 0.9,
      highOutbound: false,
    }),
    1000,
  );
  assert.equal(weak, 0.78);
});

test("maps codes to readable labels and stable label types", () => {
  assert.equal(toDisplayLabel("hub_receiver"), "Hub Receiver");
  assert.equal(toLabelType("hub_receiver"), "receiver");
  assert.equal(toDisplayLabel("high_inbound_activity"), "High Inbound Activity");
  assert.equal(toLabelType("hub"), "hub");
});

test("path terminal detection accepts legacy and display labels", () => {
  assert.equal(isPathTerminalLabel("hub", "Hub"), true);
  assert.equal(isPathTerminalLabel("receiver", "Hub Receiver"), true);
  assert.equal(isPathTerminalLabel(null, "hub_receiver"), true);
  assert.equal(
    isPathTerminalLabel("inbound_activity", "High Inbound Activity"),
    true,
  );
  assert.equal(isPathTerminalLabel(null, "high_outbound_activity"), true);
  assert.equal(isPathTerminalLabel("distributor", "Distributor"), false);
  assert.equal(isPathTerminalLabel(null, "Treasury"), false);
  assert.equal(isPathTerminalLabel(null, null), false);
});
