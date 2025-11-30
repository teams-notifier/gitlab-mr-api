-- migrate:up

-- Add fingerprint column to message ref table
ALTER TABLE gitlab_mr_api.merge_request_message_ref
ADD COLUMN last_processed_fingerprint VARCHAR(64);

-- Drop global fingerprint table (no longer needed)
DROP TABLE gitlab_mr_api.webhook_fingerprint;

-- migrate:down

-- Recreate global fingerprint table
CREATE TABLE gitlab_mr_api.webhook_fingerprint (
    fingerprint character varying(64) NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY gitlab_mr_api.webhook_fingerprint
    ADD CONSTRAINT webhook_fingerprint_pkey PRIMARY KEY (fingerprint);

-- Remove fingerprint column from message ref
ALTER TABLE gitlab_mr_api.merge_request_message_ref
DROP COLUMN last_processed_fingerprint;
