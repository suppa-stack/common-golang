package sse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// A rotation used to leave the old session ID behind in the user index, which
// then kept the user entry itself alive forever.
func TestUpdateConnectionSessionIDRemovesStaleUserIndexEntry(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connID, _, cleanup := b.SubscribeWithIDs(ctx, "old-session", "user1")

	if !b.UpdateConnectionSessionID(connID, "new-session") {
		t.Fatal("expected UpdateConnectionSessionID to return true")
	}

	b.mu.RLock()
	sessions := len(b.users["user1"])
	_, oldStillIndexed := b.sessions["old-session"]
	b.mu.RUnlock()

	if sessions != 1 {
		t.Fatalf("expected exactly 1 session in the user index, got %d", sessions)
	}
	if oldStillIndexed {
		t.Fatal("expected the old session to be dropped from the session index")
	}

	cleanup()

	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.users) != 0 {
		t.Fatalf("expected the user index to be empty after disconnect, got %v", b.users)
	}
	if len(b.sessions) != 0 {
		t.Fatalf("expected the session index to be empty after disconnect, got %v", b.sessions)
	}
	if len(b.connections) != 0 {
		t.Fatalf("expected no connection left, got %d", len(b.connections))
	}
}

// Rotation must keep working for the tabs that share a session.
func TestUpdateConnectionSessionIDKeepsSiblingConnection(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rotatedID, rotated, cleanupRotated := b.SubscribeWithIDs(ctx, "shared", "user1")
	defer cleanupRotated()
	_, sibling, cleanupSibling := b.SubscribeWithIDs(ctx, "shared", "user1")
	defer cleanupSibling()

	b.UpdateConnectionSessionID(rotatedID, "rotated")

	b.PublishToUserWithOptions("user1", Event{Data: []byte("x")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{"shared": true, "rotated": true},
	})

	if _, ok := drainOne(rotated, time.Second); !ok {
		t.Fatal("rotated connection did not receive the event")
	}
	if _, ok := drainOne(sibling, time.Second); !ok {
		t.Fatal("sibling connection on the untouched session did not receive the event")
	}
}

func TestUpdateSessionIDMovesEveryConnectionOfTheSession(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, first, cleanupFirst := b.SubscribeWithIDs(ctx, "old-session", "user1")
	defer cleanupFirst()
	_, second, cleanupSecond := b.SubscribeWithIDs(ctx, "old-session", "user1")
	defer cleanupSecond()
	_, untouched, cleanupUntouched := b.SubscribeWithIDs(ctx, "other-session", "user1")
	defer cleanupUntouched()

	if !b.UpdateSessionID("old-session", "new-session") {
		t.Fatal("expected UpdateSessionID to report a move")
	}

	// Only the new session is active: both rotated tabs must still be routable.
	b.PublishToUserWithOptions("user1", Event{Data: []byte("after")}, &PublishOptions{
		ActiveSessionIDs: map[string]bool{"new-session": true},
	})

	for name, ch := range map[string]<-chan Event{"first": first, "second": second} {
		if _, ok := drainOne(ch, time.Second); !ok {
			t.Fatalf("%s connection did not receive the event after rotation", name)
		}
	}
	if evs := drainAll(untouched); len(evs) != 0 {
		t.Fatalf("connection on another session should not have received the event, got %d", len(evs))
	}
}

func TestUpdateSessionIDUnknownOrNoop(t *testing.T) {
	b := NewBroker(8)
	tests := []struct {
		name     string
		from, to string
	}{
		{"unknown session", "missing", "new"},
		{"same id", "a", "a"},
		{"empty source", "", "new"},
		{"empty target", "a", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if b.UpdateSessionID(tc.from, tc.to) {
				t.Fatal("expected UpdateSessionID to report no move")
			}
		})
	}
}

// PublishToUsers must cost what the caller asked for, not what the server is
// currently carrying.
func TestPublishToUsersOnlyVisitsRequestedUsers(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, wanted, cleanupWanted := b.Subscribe(ctx, "user-wanted")
	defer cleanupWanted()

	for i := range 20 {
		_, _, cleanup := b.Subscribe(ctx, fmt.Sprintf("noise-%d", i))
		defer cleanup()
	}

	// A duplicate user ID must not produce a duplicate delivery.
	b.PublishToUsers([]string{"user-wanted", "user-wanted"}, Event{Data: []byte("once")})

	if evs := drainAll(wanted); len(evs) != 1 {
		t.Fatalf("expected exactly 1 event, got %d", len(evs))
	}
}

// A saturated mailbox must not silently swallow the update: the client is told
// to re-render instead of staying stale.
func TestFullMailboxQueuesResync(t *testing.T) {
	b := NewBroker(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connID, events, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	b.Publish(Event{Data: []byte("first")})  // fills the single slot
	b.Publish(Event{Data: []byte("second")}) // dropped -> queues a resync

	got, ok := drainOne(events, time.Second)
	if !ok {
		t.Fatal("expected an event")
	}
	if !strings.Contains(string(got.Data), "system.resync") {
		t.Fatalf("expected a resync event, got %q", got.Data)
	}

	if stats := b.Stats(); stats.Dropped != 1 {
		t.Fatalf("expected 1 dropped event, got %d", stats.Dropped)
	}
	if dropped, ok := b.ConnectionDrops(connID); !ok || dropped == 0 {
		t.Fatalf("expected the connection to report a drop, got %d (found=%v)", dropped, ok)
	}
}

// A burst of drops must collapse into a single resync, not one per lost event.
func TestRepeatedDropsQueueASingleResync(t *testing.T) {
	b := NewBroker(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, events, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	for range 10 {
		b.Publish(Event{Data: []byte("burst")})
	}

	resyncs := 0
	for _, e := range drainAll(events) {
		if strings.Contains(string(e.Data), "system.resync") {
			resyncs++
		}
	}
	if resyncs != 1 {
		t.Fatalf("expected exactly 1 resync event, got %d", resyncs)
	}
}

func TestStatsCountsPublishedEvents(t *testing.T) {
	b := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	b.Publish(Event{Data: []byte("a")})
	b.PublishToUser("user1", Event{Data: []byte("b")})

	stats := b.Stats()
	if stats.Connections != 1 {
		t.Fatalf("expected 1 connection, got %d", stats.Connections)
	}
	if stats.Published != 2 {
		t.Fatalf("expected 2 published events, got %d", stats.Published)
	}
	if stats.Dropped != 0 {
		t.Fatalf("expected no drop, got %d", stats.Dropped)
	}
}

// Connection IDs are allocated process-wide so one HTTP connection can register
// the same ID with several brokers without them ever colliding.
func TestConnectionIDsAreUniqueAcrossBrokers(t *testing.T) {
	first := NewBroker(8)
	second := NewBroker(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idA, _, cleanupA := first.Subscribe(ctx, "user1")
	defer cleanupA()
	idB, _, cleanupB := second.Subscribe(ctx, "user2")
	defer cleanupB()

	if idA == idB {
		t.Fatalf("expected distinct connection ids across brokers, both were %d", idA)
	}
}

func BenchmarkPublishToUser(b *testing.B) {
	broker := NewBroker(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1000 connections spread over 200 users, as a busy server would look.
	for i := range 1000 {
		user := fmt.Sprintf("user-%d", i%200)
		_, ch, cleanup := broker.SubscribeWithIDs(ctx, fmt.Sprintf("session-%d", i), user)
		defer cleanup()
		// Drain continuously so the mailboxes never saturate.
		go func(events <-chan Event) {
			for {
				select {
				case <-events:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	event := Event{Type: "app-event", Data: []byte(`{"kind":"fragment"}`)}
	filter := func(c *Connection) bool {
		_, selectors, _ := c.PageContext()
		return selectors == nil || selectors["#page-content"]
	}

	b.Run("no filter", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			broker.PublishToUser(fmt.Sprintf("user-%d", i%200), event)
		}
	})

	b.Run("with filter", func(b *testing.B) {
		opts := &PublishOptions{Filter: filter}
		for i := 0; b.Loop(); i++ {
			broker.PublishToUserWithOptions(fmt.Sprintf("user-%d", i%200), event, opts)
		}
	})
}

// BenchmarkPublishUnderPageUpdates is the shape that matters in production:
// fragments are published while /unh and /uih concurrently rewrite the page
// context of live connections.
func BenchmarkPublishUnderPageUpdates(b *testing.B) {
	broker := NewBroker(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := make([]uint64, 0, 500)
	for i := range 500 {
		id, ch, cleanup := broker.SubscribeWithIDs(ctx, fmt.Sprintf("session-%d", i), fmt.Sprintf("user-%d", i%100))
		ids = append(ids, id)
		defer cleanup()
		go func(events <-chan Event) {
			for {
				select {
				case <-events:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	selectors := map[string]bool{"#page-content": true}
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := range 4 {
		writers.Add(1)
		go func(offset int) {
			defer writers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				broker.UpdateConnectionPage(ids[(i+offset)%len(ids)], "/page/", selectors, nil)
			}
		}(w * 97)
	}

	event := Event{Type: "app-event", Data: []byte(`{"kind":"fragment"}`)}
	opts := &PublishOptions{Filter: func(c *Connection) bool {
		_, sel, _ := c.PageContext()
		return sel == nil || sel["#page-content"]
	}}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		broker.PublishToUserWithOptions(fmt.Sprintf("user-%d", i%100), event, opts)
	}
	b.StopTimer()

	close(stop)
	writers.Wait()
}

// The resync flag is cleared by the next successful send, so a client that
// recovers and later falls behind again must be told to resync a second time.
func TestDropsAfterRecoveryQueueANewResync(t *testing.T) {
	b := NewBroker(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, events, cleanup := b.Subscribe(ctx, "user1")
	defer cleanup()

	countResyncs := func() int {
		n := 0
		for _, e := range drainAll(events) {
			if strings.Contains(string(e.Data), "system.resync") {
				n++
			}
		}
		return n
	}

	// First episode: overflow the mailbox, then let the client catch up.
	for range 5 {
		b.Publish(Event{Data: []byte("burst-1")})
	}
	if got := countResyncs(); got != 1 {
		t.Fatalf("first episode: expected 1 resync, got %d", got)
	}

	// Second episode, after the mailbox drained.
	for range 5 {
		b.Publish(Event{Data: []byte("burst-2")})
	}
	if got := countResyncs(); got != 1 {
		t.Fatalf("second episode: expected a new resync, got %d", got)
	}
}
