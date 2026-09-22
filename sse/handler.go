package sse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"suppa-ahg-stack/common-golang/logger"
)

const (
	// frameWriteTimeout bounds a single SSE frame write. Without it a wedged
	// client pins the writer goroutine and its mailbox forever, because the
	// server-wide WriteTimeout has to stay off for long-lived streams.
	frameWriteTimeout = 10 * time.Second

	// defaultMailboxSize is the mailbox capacity used when no broker of a
	// connection declares a larger one.
	defaultMailboxSize = 32
)

// HandlerOptions configures the SSE handler behaviour.
// SseEventOpts is a concrete EventHandler implementation.
type SseEventOpts struct {
	HeartbeatInterval   time.Duration
	OnConnectHandler    func(*http.Request)
	OnDisconnectHandler func(*http.Request)
	Event               *Event
	Broker              *Broker
	Name                string
}

type HandlerOptions struct {
	// HeartbeatInterval sends a comment ping to keep connections alive.
	// Zero disables it.
	HeartbeatInterval time.Duration

	// OnConnect is called once a client successfully subscribes.
	OnConnect func(r *http.Request, connectionID uint64)

	// OnDisconnect is called when a client disconnects (or context is cancelled).
	OnDisconnect func(r *http.Request, connectionID uint64)

	// UserIDExtractor returns the application-level user identifier to use for
	// routing events. If nil or empty, the session cookie value is used.
	UserIDExtractor func(r *http.Request) string

	// PageResolver returns the selectors and fragment IDs present on the given page.
	// If nil, the connection starts with no known page context.
	PageResolver func(page string) (selectors, fragmentIDs map[string]bool)
}

// Handler returns an http.HandlerFunc that streams typed SSE events.
func Handler(sseEvents *SseEvents, sessionName string, opts HandlerOptions, logger *logger.FileLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if sseEvents == nil || len(sseEvents.Events) == 0 {
			logger.Error("SSE handler invoked with no event stream configured")
			http.Error(w, "streaming unavailable", http.StatusInternalServerError)
			return
		}

		cookie, err := r.Cookie(sessionName)
		if err != nil {
			logger.Error("No session cookie found during sse handler execution")
			http.Error(w, "", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		// ResponseController rather than a w.(http.Flusher) assertion: it
		// unwraps middleware that wraps the ResponseWriter, and it is also how
		// the per-write deadline below is set.
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		// Clear the read deadline inherited from Server.ReadTimeout. net/http
		// keeps a background read running on the connection to notice the
		// client going away, and that read timing out cancels the request
		// context, which would tear this stream down mid-flight. Without this,
		// a server-wide ReadTimeout cannot coexist with SSE, which is why it
		// had to be left off entirely.
		if err := rc.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			logger.Error(fmt.Sprintf("SSE: cannot clear the read deadline: %v", err))
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		sessionID := cookie.Value
		userID := sessionID
		if opts.UserIDExtractor != nil {
			if extracted := opts.UserIDExtractor(r); extracted != "" {
				userID = extracted
			}
		}

		// Every broker of this connection shares one mailbox, sized from the
		// most generous broker. A single queue is what backpressure should act
		// on, and it removes the per-broker forwarding goroutines entirely.
		mailboxSize := defaultMailboxSize
		for _, sseEvent := range sseEvents.Events {
			if size := sseEvent.GetBroker().BufferSize(); size > mailboxSize {
				mailboxSize = size
			}
		}

		var (
			connID       uint64
			mailbox      chan Event
			minHeartbeat time.Duration
		)

		for i, sseEvent := range sseEvents.Events {
			broker := sseEvent.GetBroker()

			var cleanup context.CancelFunc
			if i == 0 {
				connID, mailbox, cleanup = broker.SubscribeMailbox(r.Context(), sessionID, userID, mailboxSize)
			} else {
				// All event streams for a single HTTP connection share the same
				// connection identifier, so page-context updates and
				// per-connection publishing always target the same client
				// regardless of which broker owns a given event type.
				cleanup = broker.SubscribeWithMailbox(r.Context(), sessionID, userID, connID, mailbox)
			}

			sseEvent.OnConnect(r)

			defer func(event EventHandler, cleanupFn context.CancelFunc) {
				cleanupFn()
				event.OnDisconnect(r)
			}(sseEvent, cleanup)

			interval := sseEvent.GetHeartbeatInterval()
			if interval > 0 && (minHeartbeat == 0 || interval < minHeartbeat) {
				minHeartbeat = interval
			}
		}

		if opts.OnConnect != nil {
			opts.OnConnect(r, connID)
		}
		if opts.OnDisconnect != nil {
			defer opts.OnDisconnect(r, connID)
		}

		defer logDroppedEvents(sseEvents, connID, logger)

		// Set initial page context from Referer on every registered broker so that
		// the broker used for DOM updates (e.g. app.DomUpdateBroker) has the same
		// routing metadata as the primary broker.
		if opts.PageResolver != nil {
			page := pageFromReferer(r)
			if page != "" {
				selectors, fragmentIDs := opts.PageResolver(page)
				for _, sseEvent := range sseEvents.Events {
					sseEvent.GetBroker().UpdateConnectionPage(connID, page, selectors, fragmentIDs)
				}
			}
		}

		var frame bytes.Buffer

		// Send the connection ID to the client so it can identify itself in
		// /unh and /uih. It is written straight to the socket instead of going
		// through a broker: a full mailbox would drop it, and without it the
		// client never becomes ready and refuses to navigate.
		connPayload, err := json.Marshal(map[string]any{
			"kind": "data",
			"name": "system.connection_id",
			"data": map[string]any{
				"connection_id": strconv.FormatUint(connID, 10),
			},
		})
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to marshal connection id payload: %v", err))
			return
		}
		if err := writeFrame(w, rc, &frame, Event{Type: "app-event", Data: connPayload}); err != nil {
			return
		}

		var heartbeat <-chan time.Time
		if minHeartbeat > 0 {
			ticker := time.NewTicker(minHeartbeat)
			defer ticker.Stop()
			heartbeat = ticker.C
		}

		// Single writer loop.
		for {
			select {
			case <-r.Context().Done():
				return

			case <-heartbeat:
				if err := writeRaw(w, rc, []byte(": ping\n\n")); err != nil {
					return
				}

			case e := <-mailbox:
				if err := writeFrame(w, rc, &frame, e); err != nil {
					return
				}
			}
		}
	}
}

// logDroppedEvents reports, once per disconnect, how many events a connection
// lost. Without it a stale UI is indistinguishable from a working one.
func logDroppedEvents(sseEvents *SseEvents, connID uint64, l *logger.FileLogger) {
	if l == nil {
		return
	}
	var total uint64
	for _, sseEvent := range sseEvents.Events {
		if dropped, ok := sseEvent.GetBroker().ConnectionDrops(connID); ok {
			total += dropped
		}
	}
	if total > 0 {
		l.Warn(fmt.Sprintf("SSE connection %d dropped %d event(s); client was too slow", connID, total))
	}
}

func pageFromReferer(r *http.Request) string {
	referer := r.Header.Get("Referer")
	if referer == "" {
		return ""
	}
	u, err := url.Parse(referer)
	if err != nil {
		return ""
	}
	return u.Path
}

// writeFrame serialises a typed Event into buf and writes it as a single frame.
func writeFrame(w http.ResponseWriter, rc *http.ResponseController, buf *bytes.Buffer, e Event) error {
	buf.Reset()
	appendEvent(buf, e)
	return writeRaw(w, rc, buf.Bytes())
}

// writeRaw writes one already-serialised frame under a rolling deadline, so a
// client that stops reading is dropped rather than held open indefinitely.
func writeRaw(w http.ResponseWriter, rc *http.ResponseController, frame []byte) error {
	if err := rc.SetWriteDeadline(time.Now().Add(frameWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := w.Write(frame); err != nil {
		return err
	}
	return rc.Flush()
}

// writeEvent serialises a typed Event to the SSE wire format.
func writeEvent(w http.ResponseWriter, e Event) error {
	var buf bytes.Buffer
	appendEvent(&buf, e)
	_, err := w.Write(buf.Bytes())
	return err
}

// appendEvent writes the SSE wire representation of e into buf.
func appendEvent(buf *bytes.Buffer, e Event) {
	if e.ID != "" {
		buf.WriteString("id: ")
		buf.WriteString(e.ID)
		buf.WriteByte('\n')
	}
	if e.Type != "" {
		buf.WriteString("event: ")
		buf.WriteString(e.Type)
		buf.WriteByte('\n')
	}
	if e.Retry > 0 {
		buf.WriteString("retry: ")
		buf.WriteString(strconv.Itoa(e.Retry))
		buf.WriteByte('\n')
	}

	// A newline inside Data would end the frame early, so each line becomes its
	// own "data:" field as the SSE spec requires.
	data := e.Data
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		buf.WriteString("data: ")
		buf.Write(bytes.TrimSuffix(data[:i], []byte("\r")))
		buf.WriteByte('\n')
		data = data[i+1:]
	}
	buf.WriteString("data: ")
	buf.Write(data)
	buf.WriteString("\n\n")
}
