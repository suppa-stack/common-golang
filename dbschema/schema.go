// Package dbschema confines SQL migrations to a configured PostgreSQL namespace.
package dbschema

import (
	"bytes"
	"github.com/jackc/pgx/v5"
	"io"
	"io/fs"
	"os"
	"regexp"
	"strings"
	"time"
)

var simple = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func SearchPath(schema string) string {
	if simple.MatchString(schema) {
		return schema
	}
	return pgx.Identifier{schema}.Sanitize()
}

// Rewrite replaces identifiers, leaving comments and data strings intact. A string
// containing a schema or qualified regclass name is namespace metadata. Function
// bodies are processed as SQL; ordinary values (including COPY rows) are not.
func Rewrite(sql string, names map[string]string) string {
	var out strings.Builder
	for i := 0; i < len(sql); {
		start := i
		if strings.HasPrefix(sql[i:], "--") {
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				out.WriteString(sql[i:])
				break
			}
			i += j + 1
			out.WriteString(sql[start:i])
			continue
		}
		if strings.HasPrefix(sql[i:], "/*") {
			depth := 1
			i += 2
			for i < len(sql) && depth > 0 {
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
				} else {
					i++
				}
			}
			out.WriteString(sql[start:i])
			continue
		}
		ch := sql[i]
		if ch == '\'' || ch == '"' {
			i++
			for i < len(sql) {
				if sql[i] == ch {
					if i+1 < len(sql) && sql[i+1] == ch {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			token := sql[start:i]
			value := strings.ReplaceAll(token[1:len(token)-1], string([]byte{ch, ch}), string(ch))
			if to, ok := names[value]; ok {
				if ch == '"' {
					token = pgx.Identifier{to}.Sanitize()
				} else {
					token = "'" + to + "'"
				}
			} else if ch == '\'' {
				for from, to := range names {
					if strings.HasPrefix(value, from+".") && !strings.HasPrefix(value[len(from)+1:], "permission_check_") && !strings.HasPrefix(value[len(from)+1:], "runtime_check_") {
						token = "'" + SearchPath(to) + value[len(from):] + "'"
						break
					}
				}
			}
			out.WriteString(token)
			continue
		}
		if ch == '$' {
			if j := strings.IndexByte(sql[i+1:], '$'); j >= 0 {
				tag := sql[i : i+j+2]
				valid := true
				for _, r := range tag[1 : len(tag)-1] {
					if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
						valid = false
					}
				}
				if valid {
					if k := strings.Index(sql[i+len(tag):], tag); k >= 0 {
						body := sql[i+len(tag) : i+len(tag)+k]
						out.WriteString(tag + Rewrite(body, names) + tag)
						i += 2*len(tag) + k
						continue
					}
				}
			}
		}
		if ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' {
			i++
			for i < len(sql) && (sql[i] == '_' || sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] >= '0' && sql[i] <= '9') {
				i++
			}
			token := sql[start:i]
			if to, ok := names[token]; ok {
				token = pgx.Identifier{to}.Sanitize()
			}
			out.WriteString(token)
			continue
		}
		out.WriteByte(ch)
		i++
	}
	return out.String()
}

type mappedFS struct {
	fs.FS
	names map[string]string
}
type mappedFile struct {
	*bytes.Reader
	info fs.FileInfo
}

func (f mappedFile) Close() error               { return nil }
func (f mappedFile) Stat() (fs.FileInfo, error) { return f.info, nil }

type fileInfo struct {
	fs.FileInfo
	size int64
}

func (f fileInfo) Size() int64        { return f.size }
func (f fileInfo) ModTime() time.Time { return f.FileInfo.ModTime() }
func (f mappedFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.IsDir() {
		return file, nil
	}
	data, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return nil, err
	}
	value := []byte(Rewrite(string(data), f.names))
	return mappedFile{bytes.NewReader(value), fileInfo{info, int64(len(value))}}, nil
}

// MigrationFS leaves ordinary installations unchanged. Task runtime roles are
// injected by the supervisor and replace fixed production role identifiers.
func MigrationFS(base fs.FS, source, target string) fs.FS {
	if source == target {
		return base
	}
	names := map[string]string{source: target}
	for suffix, env := range map[string]string{"runtime": "AI_COKPIT_TASK_DATABASE_RUNTIME_ROLE", "migrator": "AI_COKPIT_TASK_DATABASE_MIGRATION_ROLE", "seed": "AI_COKPIT_TASK_DATABASE_MIGRATION_ROLE", "test": "AI_COKPIT_TASK_DATABASE_FIXTURE_ROLE", "e2e": "AI_COKPIT_TASK_DATABASE_FIXTURE_ROLE"} {
		if role := os.Getenv(env); role != "" {
			names[source+"_"+suffix] = role
		}
	}
	return mappedFS{base, names}
}
