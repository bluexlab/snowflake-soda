package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type SQLMigration struct {
	version     int64
	description string
	sql         string
	fileName    string
}

func NewSQLMigration(
	version int64,
	description string,
	sql string,
	fileName string,
) (SQLMigration, error) {
	return SQLMigration{
		version:     version,
		description: description,
		sql:         sql,
		fileName:    fileName,
	}, nil
}

func (m SQLMigration) Less(m2 SQLMigration) bool {
	return m.version < m2.version
}

func (m SQLMigration) CheckValid() error {
	if m.version <= 0 {
		return errors.New("version is not a positive number")
	}

	if m.description == "" {
		return errors.New("description is empty")
	}
	if m.sql == "" || m.fileName == "" {
		return errors.New("sql file is empty")
	}

	return nil
}

func (m SQLMigration) Execute(ctx context.Context, tx *sql.Tx) error {
	if err := m.CheckValid(); err != nil {
		return err
	}

	rawQueries := strings.Split(m.sql, ";")
	for _, query := range rawQueries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("%s %w", query, err)
		}
	}

	return nil
}
