package dbready

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerReportsSanitizedFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		rr := httptest.NewRecorder()
		Handler(func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("unbounded probe")
			}
			if fail {
				return errors.New("private database password")
			}
			return nil
		})(rr, httptest.NewRequest("GET", "/ready", nil))
		want := http.StatusOK
		if fail {
			want = http.StatusServiceUnavailable
		}
		if rr.Code != want || strings.Contains(rr.Body.String(), "password") || rr.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
		}
	}
}
func TestGrantSQLQuotesIdentifiersAndUsesOnlyRequestedPrivileges(t *testing.T) {
	sql := GrantSQL(Contract{Schema: "228_auth_app", Tables: map[string]Table{"user": {TablePrivs: []string{"SELECT"}, ColumnPrivs: map[string][]string{"email": {"SELECT", "UPDATE"}}}}, Sequences: []string{"user_id_seq"}})
	for _, want := range []string{`GRANT USAGE ON SCHEMA "228_auth_app"`, `GRANT UPDATE ("email")`, `ON "228_auth_app"."user"`, `TO :"runtime_role"`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s", want)
		}
	}
	for _, forbidden := range []string{"CREATE", "TRUNCATE", "ALL", `GRANT SELECT ("email")`} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("unexpected %s", forbidden)
		}
	}
}
