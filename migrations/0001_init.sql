CREATE TABLE chats (
    thread_id UUID PRIMARY KEY,
    organization_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX chats_organization_id_idx ON chats (organization_id);
