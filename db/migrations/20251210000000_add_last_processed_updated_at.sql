-- migrate:up
ALTER TABLE gitlab_mr_api.merge_request_message_ref
ADD COLUMN last_processed_updated_at timestamp with time zone;

-- migrate:down
ALTER TABLE gitlab_mr_api.merge_request_message_ref
DROP COLUMN last_processed_updated_at;
