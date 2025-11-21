-- migrate:up
CREATE TABLE gitlab_mr_api.webhook_fingerprint (
    fingerprint character varying(64) NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY gitlab_mr_api.webhook_fingerprint
    ADD CONSTRAINT webhook_fingerprint_pkey PRIMARY KEY (fingerprint);

-- migrate:down
DROP TABLE gitlab_mr_api.webhook_fingerprint;
