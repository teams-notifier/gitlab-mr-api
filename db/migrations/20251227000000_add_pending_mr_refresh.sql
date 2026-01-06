-- migrate:up
CREATE TABLE gitlab_mr_api.pending_mr_refresh (
    merge_request_ref_id bigint PRIMARY KEY REFERENCES gitlab_mr_api.merge_request_ref(merge_request_ref_id) ON DELETE CASCADE,
    payload_type text NOT NULL,
    first_event_at timestamp with time zone NOT NULL DEFAULT now(),
    last_event_at timestamp with time zone NOT NULL DEFAULT now(),
    process_after timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX pending_mr_refresh_process_after_idx ON gitlab_mr_api.pending_mr_refresh(process_after);

-- migrate:down
DROP TABLE gitlab_mr_api.pending_mr_refresh;
