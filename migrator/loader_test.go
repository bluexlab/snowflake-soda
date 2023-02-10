package migrator_test

import (
	"regexp"
	"testing"

	"github.com/bluexlab/snowflake-soda/migrator"
	"github.com/stretchr/testify/require"
)

func TestMigrationSQLFilePattern(t *testing.T) {
	mrx := regexp.MustCompile(migrator.MigrationSQLFilePattern)

	matches := mrx.FindAllStringSubmatch(`20230106093513_alloy_device_evaluation.up.sql`, -1)
	require.Equal(t, matches[0][1], `20230106093513`)

	matches = mrx.FindAllStringSubmatch(`202301131032029_bxpay_portal_kyc_application.up.sql`, -1)
	require.Empty(t, matches)
}
