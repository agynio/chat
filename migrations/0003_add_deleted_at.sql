ALTER TABLE chats
    ADD COLUMN deleted_at TIMESTAMPTZ;

DROP INDEX chats_org_status_created_idx;

CREATE INDEX chats_org_status_created_idx ON chats (organization_id, status, created_at DESC, thread_id DESC)
    WHERE deleted_at IS NULL;
