package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/bluexlab/snowflake-soda/migrator"
	"github.com/sirupsen/logrus"
	_ "github.com/snowflakedb/gosnowflake"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("snowflake-soda", "A table schema migration tool for Snowflake")
	migrate := app.Command("migrate", "Migration table schema")
	migrateUp := migrate.Command("up", "migrate up")
	migrateUpStep := migrateUp.Arg("step", "how many migration steps to be executed").Default("-1").Int()
	migrateDown := migrate.Command("down", "migrate down")
	migrateDownStep := migrateDown.Arg("step", "how many migration steps to be executed").Default("-1").Int()

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case migrateUp.FullCommand():
		migration(false, *migrateUpStep)
	case migrateDown.FullCommand():
		migration(true, *migrateDownStep)
	}
}

func migration(down bool, step int) {
	workDir, err := os.Getwd()
	if err != nil {
		logrus.Errorf("fail to get working directory")
		os.Exit(-1)
	}
	logrus.Info("Load migration SQLs.")
	ups, downs, err := migrator.LoadMigrations(fmt.Sprintf("%s/migrations", workDir))
	if err != nil {
		logrus.Errorf("fail to load migrations. %v", err)
		os.Exit(-1)
	}

	userName, _ := os.LookupEnv("SNOWFLAKE_USER_NAME")
	password, _ := os.LookupEnv("SNOWFLAKE_PASSWORD")
	account, _ := os.LookupEnv("SNOWFLAKE_ACCOUNT_NAME")
	dbName, _ := os.LookupEnv("SNOWFLAKE_DATABASE")
	schema, _ := os.LookupEnv("SNOWFLAKE_SCHEMA")
	warehouse, _ := os.LookupEnv("SNOWFLAKE_WAREHOUSE")
	role, _ := os.LookupEnv("SNOWFLAKE_ROLE")
	connString := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s", userName, password, account, dbName, schema, warehouse, role)
	logrus.Info("Build connection to Snowflake.")
	db, err := sql.Open("snowflake", connString)
	if err != nil {
		logrus.Error(err.Error())
		os.Exit(-1)
	}
	defer db.Close()

	migrator := migrator.NewMigrator(db, ups, downs)
	if down {
		logrus.Info("Migrate down.")
		if err := migrator.MigrateDown(step); err != nil {
			logrus.Error(err.Error())
			os.Exit(-1)
		}
	} else {
		logrus.Info("Migrate Up.")
		if err := migrator.MigrateUp(step); err != nil {
			logrus.Error(err.Error())
			os.Exit(-1)
		}
	}
	logrus.Info("Done.")
}
