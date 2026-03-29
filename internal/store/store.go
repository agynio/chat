package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	chatColumns       = "thread_id, organization_id, created_at"
	pgUniqueViolation = "23505"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func scanChat(row pgx.Row) (Chat, error) {
	var chat Chat
	if err := row.Scan(&chat.ThreadID, &chat.OrganizationID, &chat.CreatedAt); err != nil {
		return Chat{}, err
	}
	return chat, nil
}

func (s *Store) CreateChat(ctx context.Context, threadID, organizationID uuid.UUID) (Chat, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf("INSERT INTO chats (thread_id, organization_id) VALUES ($1, $2) RETURNING %s", chatColumns),
		threadID,
		organizationID,
	)
	chat, err := scanChat(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgUniqueViolation {
				return Chat{}, AlreadyExists("chat")
			}
		}
		return Chat{}, err
	}
	return chat, nil
}

func (s *Store) ListChats(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *PageCursor) (ChatListResult, error) {
	limit := NormalizePageSize(pageSize)
	query := fmt.Sprintf("SELECT %s FROM chats WHERE organization_id = $1", chatColumns)
	args := []any{organizationID}

	var cursorCreatedAt time.Time
	if cursor != nil {
		if err := s.pool.QueryRow(ctx, `SELECT created_at FROM chats WHERE thread_id = $1 AND organization_id = $2`, cursor.AfterID, organizationID).Scan(&cursorCreatedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ChatListResult{}, InvalidPageToken(fmt.Errorf("cursor not found"))
			}
			return ChatListResult{}, err
		}
		args = append(args, cursorCreatedAt, cursor.AfterID)
		query += fmt.Sprintf(" AND (created_at, thread_id) < ($%d, $%d)", len(args)-1, len(args))
	}

	args = append(args, int(limit)+1)
	query += fmt.Sprintf(" ORDER BY created_at DESC, thread_id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return ChatListResult{}, err
	}
	defer rows.Close()

	chats := make([]Chat, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(chats)) == limit {
			hasMore = true
			break
		}
		chat, err := scanChat(rows)
		if err != nil {
			return ChatListResult{}, err
		}
		chats = append(chats, chat)
		lastID = chat.ThreadID
	}
	if err := rows.Err(); err != nil {
		return ChatListResult{}, err
	}

	var nextCursor *PageCursor
	if hasMore {
		nextCursor = &PageCursor{AfterID: lastID}
	}

	return ChatListResult{Chats: chats, NextCursor: nextCursor}, nil
}
