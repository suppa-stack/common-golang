package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"suppa-ahg-stack/common-golang/sse"
)

func drainEvents(ch <-chan sse.Event) []sse.Event {
	var out []sse.Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
}

// publishRequest builds a request carrying the session cookie used as the
// user-level routing key when GetUserID is not configured.
func publishRequest(session string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/unh", nil)
	r.AddCookie(&http.Cookie{Name: "session", Value: session})
	return r
}

func TestPublishDomUpdateSkipsConnectionsOnAnotherPage(t *testing.T) {
	app := newTestInteractionApp(t)
	broker := sse.NewBroker(8)
	app.DomUpdateBroker = broker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sameID, samePage, cleanupSame := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
	defer cleanupSame()
	otherID, otherPage, cleanupOther := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
	defer cleanupOther()

	// Both tabs target #page-content; only the page tells them apart.
	selectors := map[string]bool{"#page-content": true}
	broker.UpdateConnectionPage(sameID, "/organisations/", selectors, nil)
	broker.UpdateConnectionPage(otherID, "/applications/", selectors, nil)

	if !app.PublishDomUpdate("/organisations/", "#page-content", "<p>hi</p>", publishRequest("session-a")) {
		t.Fatal("expected PublishDomUpdate to publish")
	}

	if got := drainEvents(samePage); len(got) != 1 {
		t.Fatalf("connection on /organisations/ expected 1 event, got %d", len(got))
	}
	if got := drainEvents(otherPage); len(got) != 0 {
		t.Fatalf("connection on /applications/ expected 0 events, got %d", len(got))
	}
}

func TestPublishDomUpdateReachesEveryTabOnTheSamePage(t *testing.T) {
	app := newTestInteractionApp(t)
	broker := sse.NewBroker(8)
	app.DomUpdateBroker = broker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstID, first, cleanupFirst := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
	defer cleanupFirst()
	secondID, second, cleanupSecond := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
	defer cleanupSecond()

	selectors := map[string]bool{"#page-content": true}
	broker.UpdateConnectionPage(firstID, "/organisations/", selectors, nil)
	broker.UpdateConnectionPage(secondID, "/organisations/", selectors, nil)

	app.PublishDomUpdate("/organisations/", "#page-content", "<p>hi</p>", publishRequest("session-a"))

	for name, ch := range map[string]<-chan sse.Event{"first": first, "second": second} {
		if got := drainEvents(ch); len(got) != 1 {
			t.Fatalf("%s tab expected 1 event, got %d", name, len(got))
		}
	}
}

// The stored page and the published path come from sources that disagree on
// trailing slashes and query strings, so the comparison must tolerate both.
func TestPublishDomUpdateMatchesPageAcrossPathSpellings(t *testing.T) {
	tests := []struct {
		name        string
		storedPage  string
		publishPath string
		wantEvents  int
	}{
		{"exact", "/organisations/", "/organisations/", 1},
		{"stored without trailing slash", "/organisations", "/organisations/", 1},
		{"published without trailing slash", "/organisations/", "/organisations", 1},
		{"stored with query string", "/organisations/?modal=1", "/organisations/", 1},
		{"different page", "/applications/", "/organisations/", 0},
		{"different record of a dynamic route", "/organisations/2/", "/organisations/1/", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := newTestInteractionApp(t)
			broker := sse.NewBroker(8)
			app.DomUpdateBroker = broker

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			id, events, cleanup := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
			defer cleanup()
			broker.UpdateConnectionPage(id, tc.storedPage, map[string]bool{"#page-content": true}, nil)

			app.PublishDomUpdate(tc.publishPath, "#page-content", "<p>hi</p>", publishRequest("session-a"))

			if got := drainEvents(events); len(got) != tc.wantEvents {
				t.Fatalf("expected %d event(s), got %d", tc.wantEvents, len(got))
			}
		})
	}
}

// A connection that has not reported a page yet must keep receiving events,
// otherwise updates are lost between connect and the first navigation.
func TestPublishDomUpdateDeliversWithoutPageContext(t *testing.T) {
	app := newTestInteractionApp(t)
	broker := sse.NewBroker(8)
	app.DomUpdateBroker = broker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, events, cleanup := broker.SubscribeWithIDs(ctx, "session-a", "session-a")
	defer cleanup()

	app.PublishDomUpdate("/organisations/", "#page-content", "<p>hi</p>", publishRequest("session-a"))

	if got := drainEvents(events); len(got) != 1 {
		t.Fatalf("expected 1 event for a connection without page context, got %d", len(got))
	}
}
