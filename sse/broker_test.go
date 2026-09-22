package sse

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func drainOne(ch <-chan Event, timeout time.Duration) (Event, bool) {
	select {
	case e := <-ch:
		return e, true
	case <-time.After(timeout):
		return Event{}, false
	}
}

func drainAll(ch <-chan Event) []Event {
	var out []Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
}

func TestSubscribeAndCount(t *testing.T) {
	b := NewBroker(8)
	if got := b.Count(); got != 0 {
		t.Fatalf("expected 0 connections, got %d", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	if got := b.Count(); got != 1 {
		t.Fatalf("expected 1 connection, got %d", got)
	}

	cleanup()
	if got := b.Count(); got != 0 {
		t.Fatalf("expected 0 connections after cleanup, got %d", got)
	}
	_ = ch
}

func TestPublishBroadcasts(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch1, cleanup1 := b.Subscribe(ctx, "user1")
	_, ch2, cleanup2 := b.Subscribe(ctx, "user2")
	defer cleanup1()
	defer cleanup2()

	b.Publish(Event{Type: "app-event", Data: []byte("hello")})

	for _, ch := range []<-chan Event{ch1, ch2} {
		e, ok := drainOne(ch, time.Second)
		if !ok {
			t.Fatal("expected event, got timeout")
		}
		if string(e.Data) != "hello" {
			t.Fatalf("unexpected data %q", e.Data)
		}
	}
}

func TestPublishToConnection(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id, ch, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	if ok := b.PublishToConnection(id, Event{Data: []byte("direct")}); !ok {
		t.Fatal("expected PublishToConnection to succeed")
	}

	e, ok := drainOne(ch, time.Second)
	if !ok || string(e.Data) != "direct" {
		t.Fatalf("unexpected event or timeout: %+v", e)
	}

	if ok := b.PublishToConnection(9999, Event{Data: []byte("missing")}); ok {
		t.Fatal("expected PublishToConnection to fail for unknown id")
	}
}

func TestPublishToConnections(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id1, ch1, cleanup1 := b.Subscribe(ctx, "user1")
	id2, ch2, cleanup2 := b.Subscribe(ctx, "user2")
	defer cleanup1()
	defer cleanup2()

	b.PublishToConnections([]uint64{id1, id2}, Event{Data: []byte("multi")})

	for _, ch := range []<-chan Event{ch1, ch2} {
		e, ok := drainOne(ch, time.Second)
		if !ok || string(e.Data) != "multi" {
			t.Fatalf("unexpected event or timeout: %+v", e)
		}
	}
}

func TestPublishToUserWithOptions_ActiveSessionFilter(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two sessions for the same user.
	_, ch1, cleanup1 := b.SubscribeWithIDs(ctx, "session-a", "user1")
	_, ch2, cleanup2 := b.SubscribeWithIDs(ctx, "session-b", "user1")
	defer cleanup1()
	defer cleanup2()

	b.PublishToUserWithOptions("user1", Event{Data: []byte("filtered")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{"session-a": true},
	})

	e, ok := drainOne(ch1, time.Second)
	if !ok || string(e.Data) != "filtered" {
		t.Fatalf("expected event on session-a, got %+v", e)
	}

	if evs := drainAll(ch2); len(evs) != 0 {
		t.Fatalf("expected no event on session-b, got %d", len(evs))
	}
}

func TestPublishToUserWithOptions_Filter(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch1, cleanup1 := b.SubscribeWithIDs(ctx, "session-a", "user1")
	id2, ch2, cleanup2 := b.SubscribeWithIDs(ctx, "session-b", "user1")
	defer cleanup1()
	defer cleanup2()

	b.UpdateConnectionPage(id2, "/", map[string]bool{"#page-content": true}, nil)

	b.PublishToUserWithOptions("user1", Event{Data: []byte("selective")}, &PublishOptions{
		Filter: func(c *Connection) bool {
			_, selectors, _ := c.PageContext()
			return selectors["#page-content"]
		},
	})

	if evs := drainAll(ch1); len(evs) != 0 {
		t.Fatalf("expected no event on session-a, got %d", len(evs))
	}

	e, ok := drainOne(ch2, time.Second)
	if !ok || string(e.Data) != "selective" {
		t.Fatalf("expected event on session-b, got %+v", e)
	}
}

func TestPublishToUsersWithOptions(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch1, cleanup1 := b.Subscribe(ctx, "user1")
	_, ch2, cleanup2 := b.Subscribe(ctx, "user2")
	_, ch3, cleanup3 := b.Subscribe(ctx, "user3")
	defer cleanup1()
	defer cleanup2()
	defer cleanup3()

	b.PublishToUsersWithOptions([]string{"user1", "user3"}, Event{Data: []byte("batch")}, nil)

	for _, ch := range []<-chan Event{ch1, ch3} {
		e, ok := drainOne(ch, time.Second)
		if !ok || string(e.Data) != "batch" {
			t.Fatalf("unexpected event or timeout: %+v", e)
		}
	}

	if evs := drainAll(ch2); len(evs) != 0 {
		t.Fatalf("expected no event for user2, got %d", len(evs))
	}
}

func TestUpdateConnectionPageAndHasPageContext(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id, _, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	c := b.connections[id]
	if c.HasPageContext() {
		t.Fatal("expected no page context initially")
	}

	b.UpdateConnectionPage(id, "/home", map[string]bool{"#page-content": true}, map[string]bool{"page": true})

	page, selectors, fragmentIDs := c.PageContext()
	if page != "/home" || !selectors["#page-content"] || !fragmentIDs["page"] {
		t.Fatalf("unexpected page context: %q %v %v", page, selectors, fragmentIDs)
	}
	if !c.HasPageContext() {
		t.Fatal("expected HasPageContext to be true")
	}
}

func TestUpdateSessionPage(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, cleanup1 := b.SubscribeWithIDs(ctx, "session-x", "user1")
	_, _, cleanup2 := b.SubscribeWithIDs(ctx, "session-x", "user1")
	defer cleanup1()
	defer cleanup2()

	b.UpdateSessionPage("session-x", "/dash", map[string]bool{"#dash": true}, map[string]bool{"dash": true})

	for _, c := range b.connections {
		page, selectors, _ := c.PageContext()
		if page != "/dash" || !selectors["#dash"] {
			t.Fatalf("unexpected context for connection %d", c.ConnectionID())
		}
	}
}

func TestUpdateUserID(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch, cleanup := b.Subscribe(ctx, "old-user")
	defer cleanup()

	b.UpdateUserID("old-user", "new-user")

	b.PublishToUser("new-user", Event{Data: []byte("moved")})

	e, ok := drainOne(ch, time.Second)
	if !ok || string(e.Data) != "moved" {
		t.Fatalf("expected event after UpdateUserID, got %+v", e)
	}
}

func TestSharedSessionCloseKeepsOtherConnectionRoutable(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, cleanup1 := b.SubscribeWithIDs(ctx, "shared-session", "user1")
	_, ch2, cleanup2 := b.SubscribeWithIDs(ctx, "shared-session", "user1")
	defer cleanup2()

	// Close the first connection only.
	cleanup1()

	if b.Count() != 1 {
		t.Fatalf("expected 1 remaining connection, got %d", b.Count())
	}

	// The remaining connection must still receive user-scoped events.
	b.PublishToUser("user1", Event{Data: []byte("still-here")})

	e, ok := drainOne(ch2, time.Second)
	if !ok || string(e.Data) != "still-here" {
		t.Fatalf("expected remaining connection to receive event, got %+v", e)
	}
}

func TestConcurrentPublishDisconnectDoesNotPanic(t *testing.T) {
	b := NewBroker(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numSubs = 50
	var cleanups []context.CancelFunc
	ids := make([]uint64, 0, numSubs)
	for i := 0; i < numSubs; i++ {
		id, _, cleanup := b.Subscribe(ctx, fmt.Sprintf("user-%d", i%5))
		ids = append(ids, id)
		cleanups = append(cleanups, cleanup)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// Publisher goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			b.Publish(Event{Data: []byte("broadcast")})
			b.PublishToUser(fmt.Sprintf("user-%d", i%5), Event{Data: []byte("user")})
			b.PublishToConnection(ids[i%numSubs], Event{Data: []byte("direct")})
		}
	}()

	// Page-context goroutine: /unh and /uih update the page context of a live
	// connection while fragments are being published to it.
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			b.UpdateConnectionPage(ids[i%numSubs], fmt.Sprintf("/page-%d/", i%7),
				map[string]bool{"#page-content": true}, map[string]bool{"page": true})
			b.UpdateConnectionSessionID(ids[i%numSubs], fmt.Sprintf("rotated-%d", i%3))
		}
	}()

	// Disconnect goroutine.
	go func() {
		defer wg.Done()
		for _, cleanup := range cleanups {
			cleanup()
			time.Sleep(time.Microsecond)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrency test timed out")
	}

	// If we get here without a panic (and clean under -race), publishing
	// outside the lock is safe against concurrent updates and disconnects.
}

func TestUpdateConnectionSessionID(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldSession := "old-session"
	newSession := "new-session"
	userID := "user1"

	connID, ch, cleanup := b.SubscribeWithIDs(ctx, oldSession, userID)
	defer cleanup()

	// Publish with active session set to the new token before re-association:
	// the connection should not receive the event because it still has the old session ID.
	b.PublishToUserWithOptions(userID, Event{Data: []byte("before")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{newSession: true},
	})
	if e, ok := drainOne(ch, 100*time.Millisecond); ok {
		t.Fatalf("expected no event before re-association, got %+v", e)
	}

	// Re-associate the connection to the new session ID.
	if !b.UpdateConnectionSessionID(connID, newSession) {
		t.Fatal("expected UpdateConnectionSessionID to return true")
	}

	// Now the connection should receive events scoped to the new active session.
	b.PublishToUserWithOptions(userID, Event{Data: []byte("after")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{newSession: true},
	})
	e, ok := drainOne(ch, time.Second)
	if !ok || string(e.Data) != "after" {
		t.Fatalf("expected event after re-association, got %+v", e)
	}

	// The old session should no longer route to this connection.
	b.PublishToUserWithOptions(userID, Event{Data: []byte("stale")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{oldSession: true},
	})
	if e, ok := drainOne(ch, 100*time.Millisecond); ok {
		t.Fatalf("expected no event for old session after re-association, got %+v", e)
	}
}

func TestUpdateConnectionSessionID_UnknownConnection(t *testing.T) {
	b := NewBroker(8)
	if b.UpdateConnectionSessionID(999, "session") {
		t.Fatal("expected UpdateConnectionSessionID to return false for unknown connection")
	}
}
