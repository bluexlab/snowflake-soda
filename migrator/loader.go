package migrator

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
)

const MigrationSQLFilePattern = `^(\d{14})_([\-_a-z0-9]+)?\.(up|down)\.(sql)$`

var mrx = regexp.MustCompile(MigrationSQLFilePattern)

func LoadMigrations(folder string) ([]SQLMigration, []SQLMigration, error) {
	migrationDir := os.DirFS(folder)
	dirEntries, err := fs.ReadDir(migrationDir, ".")
	if err != nil {
		return nil, nil, err
	}

	upMigrations := make([]SQLMigration, 0, 10)
	downMigrations := make([]SQLMigration, 0, 10)
	for _, d := range dirEntries {
		if d.IsDir() {
			continue
		}

		fileName := d.Name()
		matches := mrx.FindAllStringSubmatch(fileName, -1)
		if len(matches) == 0 {
			return nil, nil, fmt.Errorf(`%q is not a valid migration SQL file name`, fileName)
		}
		if len(matches[0]) != 5 {
			return nil, nil, fmt.Errorf(`%q is not a valid migration SQL file name`, fileName)
		}

		version, err := strconv.ParseInt(matches[0][1], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		description := matches[0][2]
		op := matches[0][3]
		rawQuery, err := fs.ReadFile(migrationDir, fileName)
		if err != nil {
			return nil, nil, err
		}

		m, err := NewSQLMigration(version, description, string(rawQuery), fileName)
		if err != nil {
			return nil, nil, err
		}
		if op == "up" {
			upMigrations = append(upMigrations, m)
		} else {
			downMigrations = append(downMigrations, m)
		}
	}

	sort.SliceStable(upMigrations, func(i, j int) bool {
		return upMigrations[i].Less(upMigrations[j])
	})
	sort.SliceStable(downMigrations, func(i, j int) bool {
		return downMigrations[i].Less(downMigrations[j])
	})

	// Check if migration SQL files are well paired
	if len(upMigrations) != len(downMigrations) {
		return nil, nil, errors.New("migration UPs and Downs are not well paired")
	}

	for i := range upMigrations {
		if upMigrations[i].version != downMigrations[i].version {
			return nil, nil, errors.New("migration UPs and Downs are not well paired")
		}
	}
	return upMigrations, downMigrations, nil
}
