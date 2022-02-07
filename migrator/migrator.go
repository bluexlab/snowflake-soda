package migrator

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
)

type Migrator struct {
	upMigrations   []SQLMigration
	downMigrations []SQLMigration
	db             *sql.DB
}

func NewMigrator(db *sql.DB, upMigrations, downMigrations []SQLMigration) Migrator {
	return Migrator{
		db:             db,
		upMigrations:   upMigrations,
		downMigrations: downMigrations,
	}
}

func (m Migrator) MigrateUp(step int) error {
	ctx := context.Background()
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: false,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	versions, err := m.loadVersions(ctx, tx)
	if err != nil {
		return err
	}

	nothingToMigrate := true
	for i := range m.upMigrations {
		if i < len(versions) && versions[i] == m.upMigrations[i].version {
			continue
		} else if i < len(versions) && versions[i] != m.upMigrations[i].version {
			return fmt.Errorf("schema version is inconsistent. %d vs. %d", versions[i], m.upMigrations[i].version)
		}

		// Migrate up
		if step == 0 {
			break
		}
		logrus.Infof("Execute %s.", m.upMigrations[i].fileName)
		if err := m.upMigrations[i].Execute(ctx, tx); err != nil {
			return err
		}
		if err := m.addVersion(ctx, tx, m.upMigrations[i].version); err != nil {
			return err
		}
		nothingToMigrate = false
		step--
	}
	if nothingToMigrate {
		logrus.Info("Nothing to migrate up.")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (m Migrator) MigrateDown(step int) error {
	ctx := context.Background()
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: false,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	versions, err := m.loadVersions(ctx, tx)
	if err != nil {
		return err
	}

	migrationsOnDB := make([]SQLMigration, 0, 100)
	for i := range versions {
		if i >= len(m.downMigrations) {
			return fmt.Errorf("schema has %d versions is more than the migration down SQLs %d", len(versions), len(m.downMigrations))
		}

		if versions[i] != m.downMigrations[i].version {
			return fmt.Errorf("schema version is inconsistent. %d vs. %d", versions[i], m.downMigrations[i].version)
		}

		migrationsOnDB = append(migrationsOnDB, m.downMigrations[i])
	}

	if len(migrationsOnDB) == 0 {
		logrus.Info("Nothing to migrate down.")
	}
	for i := len(migrationsOnDB) - 1; i >= 0; i-- {
		if step == 0 {
			break
		}

		logrus.Infof("Execute %s.", migrationsOnDB[i].fileName)
		if err := migrationsOnDB[i].Execute(ctx, tx); err != nil {
			return err
		}
		if err := m.removeVersion(ctx, tx, migrationsOnDB[i].version); err != nil {
			return err
		}
		step--
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (m Migrator) loadVersions(ctx context.Context, tx *sql.Tx) ([]int64, error) {
	createVersionTableQuery := `
CREATE TABLE IF NOT EXISTS schema_migration (
	version TEXT NOT NULL
)`

	if _, err := tx.ExecContext(ctx, createVersionTableQuery); err != nil {
		return nil, err
	}

	versionQuery := `SELECT version::BIGINT FROM schema_migration ORDER BY version::BIGINT ASC`
	rows, err := tx.QueryContext(ctx, versionQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	versions := make([]int64, 0, 100)
	for rows.Next() {
		v := sql.NullInt64{}
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		if !v.Valid {
			continue
		}
		versions = append(versions, v.Int64)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return versions, nil
}

func (m Migrator) addVersion(ctx context.Context, tx *sql.Tx, version int64) error {
	query := `INSERT INTO schema_migration(version) VALUES (?::TEXT)`
	if _, err := tx.ExecContext(ctx, query, version); err != nil {
		return fmt.Errorf("%s %w", query, err)
	}

	return nil
}

func (m Migrator) removeVersion(ctx context.Context, tx *sql.Tx, version int64) error {
	query := `DELETE FROM schema_migration WHERE version = (?::TEXT)`
	if _, err := tx.ExecContext(ctx, query, version); err != nil {
		return fmt.Errorf("%s %w", query, err)
	}
	return nil
}
