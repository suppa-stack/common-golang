// Package dbready checks the runtime database access contract without writing data.
package dbready

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Table struct {
	TablePrivs  []string
	ColumnPrivs map[string][]string
}

type Contract struct {
	Schema    string
	Tables    map[string]Table
	Sequences []string
}

// Check uses the application's pool identity, including effective column grants.
func Check(ctx context.Context, pool *pgxpool.Pool, c Contract) error {
	if pool == nil {
		return fmt.Errorf("database unavailable")
	}
	b := &pgx.Batch{}
	b.Queue("SELECT has_schema_privilege(current_user, $1, 'USAGE')", c.Schema)
	for name, t := range c.Tables {
		object := pgx.Identifier{c.Schema, name}.Sanitize()
		for _, priv := range t.TablePrivs {
			b.Queue("SELECT has_table_privilege(current_user, $1, $2)", object, priv)
		}
		for column, privs := range t.ColumnPrivs {
			for _, priv := range privs {
				b.Queue("SELECT has_column_privilege(current_user, $1, $2, $3)", object, column, priv)
			}
		}
	}
	for _, name := range c.Sequences {
		b.Queue("SELECT has_sequence_privilege(current_user, $1, 'USAGE')", pgx.Identifier{c.Schema, name}.Sanitize())
	}
	results := pool.SendBatch(ctx, b)
	defer results.Close()
	for i := 0; i < b.Len(); i++ {
		var ok bool
		if err := results.QueryRow().Scan(&ok); err != nil {
			return fmt.Errorf("runtime access check: %w", err)
		}
		if !ok {
			return fmt.Errorf("runtime access missing")
		}
	}
	return results.Close()
}

// Handler exposes only readiness, never SQL diagnostics or credentials.
func Handler(check func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		status := "ready"
		if check(ctx) != nil {
			status = "unavailable"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	}
}

// GrantSQL renders a parameterized, idempotent grant file for a run runtime role.
func GrantSQL(c Contract) string {
	var s strings.Builder
	s.WriteString("-- Generated runtime access contract. Apply as schema owner.\nGRANT CONNECT ON DATABASE :\"db_name\" TO :\"runtime_role\";\n")
	fmt.Fprintf(&s, "GRANT USAGE ON SCHEMA %s TO :\"runtime_role\";\n", pgx.Identifier{c.Schema}.Sanitize())
	names := make([]string, 0, len(c.Tables))
	for name := range c.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := c.Tables[name]
		object := pgx.Identifier{c.Schema, name}.Sanitize()
		if len(t.TablePrivs) > 0 {
			fmt.Fprintf(&s, "GRANT %s ON %s TO :\"runtime_role\";\n", strings.Join(t.TablePrivs, ", "), object)
		}
		columns := make([]string, 0, len(t.ColumnPrivs))
		for column := range t.ColumnPrivs {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			for _, priv := range t.ColumnPrivs[column] {
				// Whole-table privileges already cover this column.
				covered := false
				for _, tablePriv := range t.TablePrivs {
					if tablePriv == priv {
						covered = true
					}
				}
				if !covered {
					fmt.Fprintf(&s, "GRANT %s (%s) ON %s TO :\"runtime_role\";\n", priv, pgx.Identifier{column}.Sanitize(), object)
				}
			}
		}
	}
	for _, name := range c.Sequences {
		fmt.Fprintf(&s, "GRANT USAGE ON SEQUENCE %s TO :\"runtime_role\";\n", pgx.Identifier{c.Schema, name}.Sanitize())
	}
	return s.String()
}
