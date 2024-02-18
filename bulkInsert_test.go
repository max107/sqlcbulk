package sqlcbulk_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/max107/sqlcbulk"
)

func TestBulkInsert(t *testing.T) {
	t.Parallel()

	t.Run("match", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			sql      string
			expected int
		}{
			{sql: singleline, expected: 4},
			{sql: multiline, expected: 4},
		}

		for _, tc := range tests {
			matches := sqlcbulk.FindColumns(tc.sql)
			require.Len(t, matches, tc.expected)
		}
	})

	t.Run("replace", func(t *testing.T) {
		t.Parallel()

		sql := sqlcbulk.ReplaceValues(`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10, $11, $12, $13)
ON CONFLICT (code) DO UPDATE SET`, `($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`)
		require.Equal(t, `VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (code) DO UPDATE SET`, sql)
	})
}

const multiline = `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, 
$3, $4)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code;
`

const singleline = `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, $3, $4)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code;
`
