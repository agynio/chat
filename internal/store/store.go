package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	chatColumns       = "thread_id, organization_id, created_at, status, summary"
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
	if err := row.Scan(&chat.ThreadID, &chat.OrganizationID, &chat.CreatedAt, &chat.Status, &chat.Summary); err != nil {
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

func (s *Store) GetChat(ctx context.Context, threadID uuid.UUID) (Chat, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT %s FROM chats WHERE thread_id = $1", chatColumns),
		threadID,
	)
	chat, err := scanChat(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Chat{}, NotFound("chat")
		}
		return Chat{}, err
	}
	return chat, nil
}

func (s *Store) UpdateChat(ctx context.Context, threadID uuid.UUID, params UpdateChatParams) (Chat, error) {
	setClauses := make([]string, 0, 2)
	args := make([]any, 0, 3)

	if params.Status != nil {
		args = append(args, *params.Status)
		setClauses = append(setClauses, fmt.Sprintf("status = $%d", len(args)))
	}
	if params.ClearSummary {
		setClauses = append(setClauses, "summary = NULL")
	} else if params.Summary != nil {
		args = append(args, *params.Summary)
		setClauses = append(setClauses, fmt.Sprintf("summary = $%d", len(args)))
	}

	if len(setClauses) == 0 {
		return s.GetChat(ctx, threadID)
	}

	args = append(args, threadID)
	query := fmt.Sprintf("UPDATE chats SET %s WHERE thread_id = $%d RETURNING %s", strings.Join(setClauses, ", "), len(args), chatColumns)
	row := s.pool.QueryRow(ctx, query, args...)
	chat, err := scanChat(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Chat{}, NotFound("chat")
		}
		return Chat{}, err
	}
	return chat, nil
}

func (s *Store) ListChats(ctx context.Context, organizationID uuid.UUID, filter ChatListFilter, pageSize int32, cursor *PageCursor) (ChatListResult, error) {
	limit := NormalizePageSize(pageSize)
	query := fmt.Sprintf("SELECT %s FROM chats WHERE organization_id = $1", chatColumns)
	args := []any{organizationID}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		query += fmt.Sprintf(" AND status = $%d", len(args))
	}

	var cursorCreatedAt time.Time
	if cursor != nil {
		cursorQuery := `SELECT created_at FROM chats WHERE thread_id = $1 AND organization_id = $2`
		cursorArgs := []any{cursor.AfterID, organizationID}
		if filter.Status != nil {
			cursorArgs = append(cursorArgs, *filter.Status)
			cursorQuery += fmt.Sprintf(" AND status = $%d", len(cursorArgs))
		}
		if err := s.pool.QueryRow(ctx, cursorQuery, cursorArgs...).Scan(&cursorCreatedAt); err != nil {
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
