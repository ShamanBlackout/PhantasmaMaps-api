export const DEFAULT_LABEL_SOURCE = "heuristic_rubric_v1";
export const DEFAULT_LABEL_VERSION = "label-rubric-v2";

export type CandidateCode =
  | "hub"
  | "distributor"
  | "hub_receiver"
  | "high_inbound_activity"
  | "high_outbound_activity"
  | "normal";

interface LabelDefinition {
  label: string;
  labelType: string;
  baseConfidence: number;
}

// `label` is user-facing (rendered as the wallet name); `labelType` is the stable machine code.
export const LABEL_DEFINITIONS: Record<CandidateCode, LabelDefinition> = {
  hub: { label: "Hub", labelType: "hub", baseConfidence: 0.9 },
  distributor: {
    label: "Distributor",
    labelType: "distributor",
    baseConfidence: 0.86,
  },
  hub_receiver: {
    label: "Hub Receiver",
    labelType: "receiver",
    baseConfidence: 0.86,
  },
  high_inbound_activity: {
    label: "High Inbound Activity",
    labelType: "inbound_activity",
    baseConfidence: 0.78,
  },
  high_outbound_activity: {
    label: "High Outbound Activity",
    labelType: "outbound_activity",
    baseConfidence: 0.78,
  },
  normal: { label: "Normal", labelType: "normal", baseConfidence: 0.35 },
};

export interface LabelingMetrics {
  inTxCount: number;
  outTxCount: number;
  inUniqueCounterparties: number;
  outUniqueCounterparties: number;
  inVolume: number;
  outVolume: number;
  inPercentRank: number;
  outPercentRank: number;
  inZScoreLog: number | null;
  outZScoreLog: number | null;
  inMadScoreLog: number | null;
  outMadScoreLog: number | null;
  highInbound: boolean;
  highOutbound: boolean;
  highInCounterparties: boolean;
  highOutCounterparties: boolean;
}

export interface RubricGuards {
  // Percentile/z-score flags are meaningless when a token has only a handful of wallets.
  minPopulation: number;
  minDirectionalTx: number;
  minCounterparties: number;
}

export const DEFAULT_RUBRIC_GUARDS: RubricGuards = {
  minPopulation: 10,
  minDirectionalTx: 3,
  minCounterparties: 2,
};

const HUB_MAX_VOLUME_IMBALANCE = 0.35;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function classifyCandidate(
  metrics: LabelingMetrics,
  populationSize: number,
  guards: RubricGuards = DEFAULT_RUBRIC_GUARDS,
): CandidateCode {
  if (populationSize < guards.minPopulation) {
    return "normal";
  }

  const inboundActive =
    metrics.highInbound && metrics.inTxCount >= guards.minDirectionalTx;
  const outboundActive =
    metrics.highOutbound && metrics.outTxCount >= guards.minDirectionalTx;
  const inboundSpread =
    metrics.highInCounterparties &&
    metrics.inUniqueCounterparties >= guards.minCounterparties;
  const outboundSpread =
    metrics.highOutCounterparties &&
    metrics.outUniqueCounterparties >= guards.minCounterparties;

  const maxVolume = Math.max(metrics.inVolume, metrics.outVolume);
  const balancedVolume =
    metrics.inVolume > 0 &&
    metrics.outVolume > 0 &&
    Math.abs(metrics.inVolume - metrics.outVolume) / maxVolume <=
      HUB_MAX_VOLUME_IMBALANCE;

  if (
    inboundActive &&
    outboundActive &&
    inboundSpread &&
    outboundSpread &&
    balancedVolume
  ) {
    return "hub";
  }
  if (outboundActive && outboundSpread) return "distributor";
  if (inboundActive && inboundSpread) return "hub_receiver";
  if (inboundActive) return "high_inbound_activity";
  if (outboundActive) return "high_outbound_activity";
  return "normal";
}

export function computeLabelConfidence(
  code: CandidateCode,
  metrics: LabelingMetrics,
  populationSize: number,
): number {
  let confidence = LABEL_DEFINITIONS[code].baseConfidence;

  const inMad = metrics.inMadScoreLog ?? 0;
  const outMad = metrics.outMadScoreLog ?? 0;
  const inZ = metrics.inZScoreLog ?? 0;
  const outZ = metrics.outZScoreLog ?? 0;

  if (Math.max(inMad, outMad) >= 3) confidence += 0.05;
  if (Math.max(inZ, outZ) >= 2) confidence += 0.03;
  if (Math.max(metrics.inPercentRank, metrics.outPercentRank) >= 0.99) {
    confidence += 0.03;
  }
  if (metrics.highInbound && metrics.highOutbound) confidence += 0.02;

  confidence = Math.min(confidence, 0.99);

  // Small token populations produce unstable percentile/robust-score thresholds.
  if (populationSize < 30) confidence -= 0.08;
  else if (populationSize < 100) confidence -= 0.03;

  return clamp(Number(confidence.toFixed(4)), 0, 0.99);
}

export function toDisplayLabel(code: CandidateCode): string {
  return LABEL_DEFINITIONS[code].label;
}

export function toLabelType(code: CandidateCode): string {
  return LABEL_DEFINITIONS[code].labelType;
}

export function isPathTerminalLabel(
  labelType: string | null | undefined,
  label: string | null | undefined,
): boolean {
  const normalizedType = String(labelType || "").trim().toLowerCase();
  const normalizedLabel = String(label || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");

  return [normalizedType, normalizedLabel].some(
    (value) =>
      value.includes("hub") ||
      value === "receiver" ||
      value.includes("high_inbound") ||
      value.includes("high_outbound") ||
      value === "inbound_activity" ||
      value === "outbound_activity",
  );
}
