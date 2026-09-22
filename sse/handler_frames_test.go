package sse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"suppa-ahg-stack/common-golang/logger"
)

// failingWriter is an http.ResponseWriter whose body writes always fail.
type failingWriter struct {
	header http.Header
}

func (f *failingWriter) Header() http.Header {
	if f.header == nil {
		f.header = make(http.Header)
	}
	return f.header
}

func (f *failingWriter) Write([]byte) (int, error) { return 0, errors.New("connection reset") }
func (f *failingWriter) WriteHeader(int)           {}

// testLogger builds a usable logger: a zero-value FileLogger panics on first use.
func testLogger(t *testing.T) *logger.FileLogger {
	t.Helper()
	l, err := logger.NewFileLogger(logger.LogConfig{
		Filename: filepath.Join(t.TempDir(), "sse.log"),
		Level:    0,
	})
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestWriteEventPropagatesWriteError(t *testing.T) {
	err := writeEvent(&failingWriter{}, Event{Type: "app-event", Data: []byte("payload")})
	if err == nil {
		t.Fatal("expected writeEvent to report the write error")
	}
}

func TestAppendEventWireFormat(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "data only",
			event: Event{Data: []byte("hello")},
			want:  "data: hello\n\n",
		},
		{
			name:  "all fields",
			event: Event{ID: "7", Type: "app-event", Retry: 1500, Data: []byte("hello")},
			want:  "id: 7\nevent: app-event\nretry: 1500\ndata: hello\n\n",
		},
		{
			// A raw newline used to terminate the frame early and corrupt the
			// rest of the stream.
			name:  "multi-line data becomes one data field per line",
			event: Event{Data: []byte("first\nsecond\nthird")},
			want:  "data: first\ndata: second\ndata: third\n\n",
		},
		{
			name:  "CRLF data",
			event: Event{Data: []byte("first\r\nsecond")},
			want:  "data: first\ndata: second\n\n",
		},
		{
			name:  "empty data",
			event: Event{Type: "app-event"},
			want:  "event: app-event\ndata: \n\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf strings.Builder
			w := httptest.NewRecorder()
			if err := writeEvent(w, tc.event); err != nil {
				t.Fatalf("writeEvent: %v", err)
			}
			buf.WriteString(w.Body.String())
			if got := buf.String(); got != tc.want {
				t.Fatalf("frame = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHandlerRejectsEmptyEventList(t *testing.T) {
	l := testLogger(t)
	tests := []struct {
		name   string
		events *SseEvents
	}{
		{"nil", nil},
		{"empty", &SseEvents{Events: nil}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/sse-events", nil)
			r.AddCookie(&http.Cookie{Name: "session", Value: "s"})

			Handler(tc.events, "session", HandlerOptions{}, l)(w, r)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
			}
		})
	}
}

// lockedRecorder serialises access to the recorder so the test goroutine can
// read the body while the handler is still writing.
type lockedRecorder struct {
	mu sync.Mutex
	*httptest.ResponseRecorder
}

func (l *lockedRecorder) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ResponseRecorder.Write(b)
}

func (l *lockedRecorder) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ResponseRecorder.Flush()
}

func (l *lockedRecorder) body() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ResponseRecorder.Body.String()
}

// The connection ID must never go through a broker: a saturated mailbox would
// drop it, and without it the client never becomes ready and refuses to navigate.
func TestConnectionIDBypassesTheBroker(t *testing.T) {
	// A buffer of 1, as the dev-reload broker uses.
	broker := NewBroker(1)
	events := &SseEvents{Events: []EventHandler{&SseEventOpts{Broker: broker, Name: "reload"}}}

	w := &lockedRecorder{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest(http.MethodGet, "/sse-events", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: "test-session"})

	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		Handler(events, "session", HandlerOptions{}, testLogger(t))(w, r)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		if strings.Contains(w.body(), "system.connection_id") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("connection id frame never written; body = %q", w.body())
		default:
		}
	}

	// Nothing was published through the broker to deliver it.
	if stats := broker.Stats(); stats.Published != 0 {
		t.Fatalf("expected the connection id to bypass the broker, but %d event(s) were published", stats.Published)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after cancellation")
	}
}
