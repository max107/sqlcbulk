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

	t.Run("find columns", func(t *testing.T) {
		t.Parallel()

		{
			require.Equal(t, []string{"$1", "$2", "$3"}, sqlcbulk.FindColumns(`with old as (select old.is_resolved
             from event as old
             where old.event_id = $4)
insert
into event (project_id,
                   event_id,
                   environment)
values ($1,
        $2,
        $3)
on conflict (hash) do update set count       = excluded.count + sentry_event.count,
                                 is_resolved = false,
                                 created_at  = now()
returning event_id, project_id, environment, (select is_resolved from old) as is_regression`))
		}

		{
			require.Equal(t, []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"}, sqlcbulk.FindColumns(`-- name: CreateItem :batchexec
INSERT INTO
    item (
        payment_id,
        name,
        qty,
        payment_object,
        payment_method,
        vat,
        price,
        agent
    )
VALUES
    (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8
    )
`))
		}
	})

	t.Run("replace_singleline", func(t *testing.T) {
		t.Parallel()

		sql := sqlcbulk.ReplaceValues(singleline, `($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)`)
		require.Equal(t, `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code`, sql)
	})

	t.Run("replace_casesensitive", func(t *testing.T) {
		t.Parallel()

		sql := sqlcbulk.ReplaceValues(casesensitive, `($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12), ($13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`)
		require.Equal(t, `-- name: CreateProduct :one
insert into products (uuid,
                      name,
                      code,
                      price,
                      category_id,
                      weight,
                      width,
                      length,
                      height,
                      is_published,
                      parent_id,
                      variables)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12), ($13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24) returning id, uuid, code, name, description, category_id, price, is_remote, qty, weight, width, length, height, images, parent_id, variables, is_published, created_at`, sql)
	})

	t.Run("replace_multiline", func(t *testing.T) {
		t.Parallel()

		sql := sqlcbulk.ReplaceValues(multiline, `($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)`)
		require.Equal(t, `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code`, sql)
	})

	t.Run("replace_multiline_where", func(t *testing.T) {
		t.Parallel()

		sql := sqlcbulk.ReplaceValues(where, `($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)`)
		require.Equal(t, `-- name: CreateAddresses :batchexec
INSERT INTO foobar AS ra (id, total, amount, version, created_at)
VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)
ON CONFLICT (id, version) DO UPDATE SET created_at = EXCLUDED.created_at,
                                        total      = EXCLUDED.total,
                                        amount     = EXCLUDED.amount
WHERE ra.created_at < EXCLUDED.created_at
  AND (ra.total != EXCLUDED.total OR ra.amount != EXCLUDED.amount)
RETURNING id, total, amount, version, created_at`, sql)
	})
}

const where = `-- name: CreateAddresses :batchexec
INSERT INTO foobar AS ra (id, total, amount, version, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id, version) DO UPDATE SET created_at = EXCLUDED.created_at,
                                        total      = EXCLUDED.total,
                                        amount     = EXCLUDED.amount
WHERE ra.created_at < EXCLUDED.created_at
  AND (ra.total != EXCLUDED.total OR ra.amount != EXCLUDED.amount)
RETURNING id, total, amount, version, created_at`

const multiline = `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, 
$3, $4)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code`

const singleline = `-- name: CreateCity :batchexec
INSERT INTO cdek_city (code, city, fias_guid, city_uuid)
VALUES ($1, $2, $3, $4)
ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code`

const casesensitive = `-- name: CreateProduct :one
insert into products (uuid,
                      name,
                      code,
                      price,
                      category_id,
                      weight,
                      width,
                      length,
                      height,
                      is_published,
                      parent_id,
                      variables)
values ($1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12) returning id, uuid, code, name, description, category_id, price, is_remote, qty, weight, width, length, height, images, parent_id, variables, is_published, created_at`
