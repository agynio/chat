ALTER TABLE chats
    ADD COLUMN status TEXT NOT NULL DEFAULT 'open',
    ADD COLUMN summary TEXT;

CREATE INDEX chats_org_status_created_idx ON chats (organization_id, status, created_at DESC, thread_id DESC);
