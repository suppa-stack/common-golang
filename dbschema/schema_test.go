package dbschema

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

func TestNumericNamespaceMigration(t *testing.T) {
	t.Setenv("AI_COKPIT_TASK_DATABASE_RUNTIME_ROLE", "ai_cokpit_run_example")
	source := `-- auth_app must stay in this comment
DO $body$ BEGIN PERFORM auth_app.f(); PERFORM has_schema_privilege('auth_app', 'USAGE'); END $body$;
GRANT SELECT ON auth_app."user" TO auth_app_runtime;
SELECT 'auth_app.user'::regclass;
SELECT set_config('auth_app.permission_check_role', 'r', false);`
	base := fstest.MapFS{"001.sql": &fstest.MapFile{Data: []byte(source)}}
	mapped := MigrationFS(base, "auth_app", "228_auth_app")
	data, err := fs.ReadFile(mapped, "001.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`-- auth_app must stay`, `"228_auth_app".f()`, `'228_auth_app'`, `TO "ai_cokpit_run_example"`, `'"228_auth_app".user'`, `'auth_app.permission_check_role'`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("missing %s in %s", want, data)
		}
	}
	files, err := fs.Glob(mapped, "*.sql")
	if err != nil || len(files) != 1 {
		t.Fatalf("glob %v %v", files, err)
	}
	original, _ := fs.ReadFile(MigrationFS(base, "auth_app", "auth_app"), "001.sql")
	if string(original) != source {
		t.Fatal("ordinary migrations changed")
	}
}
