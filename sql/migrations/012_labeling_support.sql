-- ============================================================
-- 012_labeling_support.sql
-- Label storage, provenance, and score snapshots
-- ============================================================

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_type TEXT;

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_source TEXT;

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_confidence NUMERIC(5,4);

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_evidence JSONB;

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_version TEXT;

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS label_updated_at TIMESTAMP;

UPDATE nodes
   SET label_evidence = '{}'::jsonb
 WHERE label_evidence IS NULL;

ALTER TABLE nodes
    ALTER COLUMN label_evidence SET DEFAULT '{}'::jsonb;

ALTER TABLE nodes
    ALTER COLUMN label_evidence SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'chk_nodes_label_confidence_range'
    ) THEN
        ALTER TABLE nodes
            ADD CONSTRAINT chk_nodes_label_confidence_range
            CHECK (
                label_confidence IS NULL
                OR (label_confidence >= 0::numeric AND label_confidence <= 1::numeric)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_nodes_token_label_type
    ON nodes(token_symbol, label_type)
    WHERE label_type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_nodes_token_label_confidence
    ON nodes(token_symbol, label_confidence DESC)
    WHERE label_confidence IS NOT NULL;

CREATE TABLE IF NOT EXISTS node_label_scores (
    token_symbol TEXT NOT NULL,
    address TEXT NOT NULL,
    window_days INTEGER NOT NULL DEFAULT 30,
    in_tx_count BIGINT NOT NULL DEFAULT 0,
    out_tx_count BIGINT NOT NULL DEFAULT 0,
    in_unique_counterparties BIGINT NOT NULL DEFAULT 0,
    out_unique_counterparties BIGINT NOT NULL DEFAULT 0,
    in_volume NUMERIC NOT NULL DEFAULT 0,
    out_volume NUMERIC NOT NULL DEFAULT 0,
    in_percent_rank NUMERIC,
    out_percent_rank NUMERIC,
    in_z_score_log NUMERIC,
    out_z_score_log NUMERIC,
    in_mad_score_log NUMERIC,
    out_mad_score_log NUMERIC,
    high_inbound BOOLEAN NOT NULL DEFAULT FALSE,
    high_outbound BOOLEAN NOT NULL DEFAULT FALSE,
    high_in_counterparties BOOLEAN NOT NULL DEFAULT FALSE,
    high_out_counterparties BOOLEAN NOT NULL DEFAULT FALSE,
    candidate_label TEXT,
    computed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_symbol, address, window_days),
    FOREIGN KEY (address, token_symbol)
        REFERENCES nodes(address, token_symbol)
        ON DELETE CASCADE,
    CHECK (window_days >= 0)
);

CREATE INDEX IF NOT EXISTS idx_node_label_scores_candidate
    ON node_label_scores(token_symbol, candidate_label);

CREATE INDEX IF NOT EXISTS idx_node_label_scores_computed_at
    ON node_label_scores(computed_at DESC);

CREATE TABLE IF NOT EXISTS node_label_history (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    previous_label TEXT,
    previous_label_type TEXT,
    new_label TEXT,
    new_label_type TEXT,
    label_source TEXT,
    label_confidence NUMERIC(5,4),
    label_evidence JSONB,
    label_version TEXT,
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    changed_by TEXT,
    FOREIGN KEY (address, token_symbol)
        REFERENCES nodes(address, token_symbol)
        ON DELETE CASCADE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'chk_node_label_history_confidence_range'
    ) THEN
        ALTER TABLE node_label_history
            ADD CONSTRAINT chk_node_label_history_confidence_range
            CHECK (
                label_confidence IS NULL
                OR (label_confidence >= 0::numeric AND label_confidence <= 1::numeric)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_node_label_history_token_address
    ON node_label_history(token_symbol, address, changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_node_label_history_changed_at
    ON node_label_history(changed_at DESC);
