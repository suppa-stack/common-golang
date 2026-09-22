package sse

import (
	"context"
	"sync"
	"sync/atomic"
)

// Event is a typed SSE message.
type Event struct {
	ID    string // maps to SSE "id:" field
	Type  string // maps to SSE "event:" field
	Data  []byte
	Retry int // optional reconnect hint in ms
}

// resyncEvent tells the client that at least one event was dropped for its
// connection and that it must re-request its current page. Replaying a backlog
// of DOM fragments would be strictly worse than rendering the page once.
var resyncEvent = Event{
	Type: "app-event",
	Data: []byte(`{"kind":"data","name":"system.resync","data":{"reason":"dropped"}}`),
}

// connectionIDs allocates connection identifiers that are unique across every
// broker in the process, so one HTTP connection can register the same ID with
// several brokers without them ever colliding.
var connectionIDs atomic.Uint64

// connSlicePool recycles the target slices used to snapshot connections before
// publishing, keeping the fan-out path allocation-free.
var connSlicePool = sync.Pool{
	New: func() any {
		s := make([]*Connection, 0, 64)
		return &s
	},
}

func getConnSlice() *[]*Connection {
	return connSlicePool.Get().(*[]*Connection)
}

func putConnSlice(s *[]*Connection) {
	*s = (*s)[:0]
	connSlicePool.Put(s)
}

// identity is the immutable routing identity of a connection. It is swapped
// atomically on token rotation so readers never need the broker lock.
type identity struct {
	sessionID string
	userID    string
}

// pageContext is the immutable page routing metadata of a connection. It is
// swapped atomically so publishers can evaluate filters outside the lock.
type pageContext struct {
	page        string
	selectors   map[string]bool
	fragmentIDs map[string]bool
}

// Connection holds a subscriber's send channel and routing metadata.
type Connection struct {
	id     uint64
	ch     chan Event
	cancel context.CancelFunc

	ident   atomic.Pointer[identity]
	pageCtx atomic.Pointer[pageContext]

	// dropped counts events discarded because the mailbox was full.
	dropped atomic.Uint64
	// resyncPending is set once a resync event has been queued, so a burst of
	// drops produces a single resync instead of one per lost event.
	resyncPending atomic.Bool
}

// PageContext returns the current page and fragment sets for a connection.
func (c *Connection) PageContext() (page string, selectors, fragmentIDs map[string]bool) {
	pc := c.pageCtx.Load()
	if pc == nil {
		return "", nil, nil
	}
	return pc.page, pc.selectors, pc.fragmentIDs
}

// HasPageContext reports whether the connection has received an explicit page
// context. When false, fragment filtering should fall back to delivery so that
// events are not silently lost before the first page update arrives.
func (c *Connection) HasPageContext() bool {
	pc := c.pageCtx.Load()
	return pc != nil && pc.page != ""
}

// ConnectionID returns the unique connection identifier.
func (c *Connection) ConnectionID() uint64 {
	return c.id
}

// SessionID returns the session identifier for the connection.
func (c *Connection) SessionID() string {
	if id := c.ident.Load(); id != nil {
		return id.sessionID
	}
	return ""
}

// UserID returns the user identifier for the connection.
func (c *Connection) UserID() string {
	if id := c.ident.Load(); id != nil {
		return id.userID
	}
	return ""
}

// DroppedEvents returns how many events were discarded for this connection
// because its mailbox was full.
func (c *Connection) DroppedEvents() uint64 {
	return c.dropped.Load()
}

// PublishOptions configures user-scoped publishing.
type PublishOptions struct {
	// ActiveSessionIDs restricts publishing to sessions whose ID is in this set.
	// A nil map disables the active-session filter.
	ActiveSessionIDs map[string]bool
	// Filter is an optional per-connection predicate.
	Filter func(*Connection) bool
}

// Stats is a point-in-time snapshot of broker activity.
type Stats struct {
	Connections int
	Published   uint64
	Dropped     uint64
}

// Broker manages pub/sub for typed SSE events with user/session/connection routing.
type Broker struct {
	mu sync.RWMutex

	// connectionID -> connection
	connections map[uint64]*Connection

	// sessionID -> set of connection IDs
	sessions map[string]map[uint64]bool

	// userID -> set of session IDs
	users map[string]map[string]bool

	buf int // channel buffer size per client

	published atomic.Uint64
	dropped   atomic.Uint64
}

// NewBroker creates a broker with the given per-connection channel buffer size.
func NewBroker(bufSize int) *Broker {
	if bufSize <= 0 {
		bufSize = 32
	}
	return &Broker{
		connections: make(map[uint64]*Connection),
		sessions:    make(map[string]map[uint64]bool),
		users:       make(map[string]map[string]bool),
		buf:         bufSize,
	}
}

// BufferSize returns the per-connection mailbox capacity configured for this
// broker. The SSE handler uses it to size the mailbox shared by every broker
// serving one HTTP connection.
func (b *Broker) BufferSize() int {
	return b.buf
}

// Subscribe registers a client keyed by userID.
// The provided userID is also used as the sessionID.
// It is kept for backward compatibility; prefer SubscribeWithIDs.
func (b *Broker) Subscribe(ctx context.Context, userID string) (uint64, <-chan Event, context.CancelFunc) {
	return b.SubscribeWithIDs(ctx, userID, userID)
}

// SubscribeWithIDs registers a client keyed by both sessionID and userID.
func (b *Broker) SubscribeWithIDs(ctx context.Context, sessionID, userID string) (uint64, <-chan Event, context.CancelFunc) {
	id, ch, cancel := b.SubscribeMailbox(ctx, sessionID, userID, 0)
	return id, ch, cancel
}

// SubscribeMailbox registers a client and returns the connection mailbox, which
// the other brokers serving the same HTTP connection join through
// SubscribeWithMailbox. A bufSize of 0 falls back to the broker's own size.
func (b *Broker) SubscribeMailbox(ctx context.Context, sessionID, userID string, bufSize int) (uint64, chan Event, context.CancelFunc) {
	if bufSize <= 0 {
		bufSize = b.buf
	}
	id := connectionIDs.Add(1)
	ch := make(chan Event, bufSize)
	return id, ch, b.register(ctx, sessionID, userID, id, ch)
}

// SubscribeWithMailbox registers a client on an already allocated mailbox under
// an externally allocated connection identifier. Sharing one mailbox across all
// of a connection's brokers means the handler needs neither a forwarding
// goroutine nor a merge step per broker, and gives the connection a single
// coherent backpressure buffer.
func (b *Broker) SubscribeWithMailbox(ctx context.Context, sessionID, userID string, id uint64, mailbox chan Event) context.CancelFunc {
	return b.register(ctx, sessionID, userID, id, mailbox)
}

// SubscribeWithConnectionID registers a client with an externally allocated
// connection identifier and its own mailbox.
// Prefer SubscribeWithMailbox, which shares a single mailbox per connection.
func (b *Broker) SubscribeWithConnectionID(ctx context.Context, sessionID, userID string, id uint64) (<-chan Event, context.CancelFunc) {
	ch := make(chan Event, b.buf)
	return ch, b.register(ctx, sessionID, userID, id, ch)
}

func (b *Broker) register(ctx context.Context, sessionID, userID string, id uint64, ch chan Event) context.CancelFunc {
	_, cancel := context.WithCancel(ctx)
	c := &Connection{id: id, ch: ch, cancel: cancel}
	c.ident.Store(&identity{sessionID: sessionID, userID: userID})
	c.pageCtx.Store(&pageContext{})

	b.mu.Lock()
	b.connections[id] = c
	if b.sessions[sessionID] == nil {
		b.sessions[sessionID] = make(map[uint64]bool)
	}
	b.sessions[sessionID][id] = true
	if b.users[userID] == nil {
		b.users[userID] = make(map[string]bool)
	}
	b.users[userID][sessionID] = true
	b.mu.Unlock()

	return func() {
		cancel()
		b.removeConnection(id)
	}
}

// UpdateConnectionSessionID re-associates a single connection from its old
// sessionID to a new sessionID. Used when the auth layer rotates the session
// token while the SSE connection (opened with the old token) is still active.
func (b *Broker) UpdateConnectionSessionID(connID uint64, newSessionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.connections[connID]
	if !ok {
		return false
	}
	b.reassignLocked(c, newSessionID)
	return true
}

// UpdateSessionID re-associates every connection of oldSessionID to
// newSessionID. Unlike UpdateConnectionSessionID it does not depend on the
// client reporting its connection ID, so it also covers rotations that happen
// on a plain page load or while the SSE stream is reconnecting.
// It reports whether at least one connection was moved.
func (b *Broker) UpdateSessionID(oldSessionID, newSessionID string) bool {
	if oldSessionID == "" || newSessionID == "" || oldSessionID == newSessionID {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ids := b.sessions[oldSessionID]
	if len(ids) == 0 {
		return false
	}

	conns := make([]*Connection, 0, len(ids))
	for id := range ids {
		if c, ok := b.connections[id]; ok {
			conns = append(conns, c)
		}
	}
	for _, c := range conns {
		b.reassignLocked(c, newSessionID)
	}
	return len(conns) > 0
}

// reassignLocked moves one connection to newSessionID and keeps the session and
// user indexes consistent. b.mu must be held for writing.
func (b *Broker) reassignLocked(c *Connection, newSessionID string) {
	ident := c.ident.Load()
	if ident.sessionID == newSessionID {
		return
	}

	if ident.sessionID != "" {
		delete(b.sessions[ident.sessionID], c.id)
		if len(b.sessions[ident.sessionID]) == 0 {
			delete(b.sessions, ident.sessionID)
			// No connection is left on the old session, so drop it from the
			// user index as well. Keeping it would leak an entry per rotation
			// and make every publish to this user walk a dead session.
			if ident.userID != "" {
				delete(b.users[ident.userID], ident.sessionID)
			}
		}
	}

	c.ident.Store(&identity{sessionID: newSessionID, userID: ident.userID})

	if b.sessions[newSessionID] == nil {
		b.sessions[newSessionID] = make(map[uint64]bool)
	}
	b.sessions[newSessionID][c.id] = true

	if ident.userID != "" {
		if b.users[ident.userID] == nil {
			b.users[ident.userID] = make(map[string]bool)
		}
		b.users[ident.userID][newSessionID] = true
	}
}

// UpdateConnectionPage updates the page context for a single connection.
func (b *Broker) UpdateConnectionPage(connID uint64, page string, selectors, fragmentIDs map[string]bool) bool {
	b.mu.RLock()
	c, ok := b.connections[connID]
	b.mu.RUnlock()
	if !ok {
		return false
	}

	c.pageCtx.Store(&pageContext{page: page, selectors: selectors, fragmentIDs: fragmentIDs})
	return true
}

// UpdateSessionPage updates the page context for every connection of a session.
func (b *Broker) UpdateSessionPage(sessionID, page string, selectors, fragmentIDs map[string]bool) bool {
	targets := getConnSlice()
	defer putConnSlice(targets)

	b.mu.RLock()
	ids, ok := b.sessions[sessionID]
	for id := range ids {
		if c, found := b.connections[id]; found {
			*targets = append(*targets, c)
		}
	}
	b.mu.RUnlock()

	if !ok {
		return false
	}

	pc := &pageContext{page: page, selectors: selectors, fragmentIDs: fragmentIDs}
	for _, c := range *targets {
		c.pageCtx.Store(pc)
	}
	return true
}

// send performs a non-blocking send to one connection. A full mailbox means the
// client cannot keep up: the event is dropped and a resync is queued so the
// client re-renders instead of staying silently stale.
func (b *Broker) send(c *Connection, e Event) bool {
	select {
	case c.ch <- e:
		b.published.Add(1)
		// Room again in the mailbox means any queued resync was consumed.
		c.resyncPending.Store(false)
		return true
	default:
		b.dropped.Add(1)
		c.dropped.Add(1)
		b.queueResync(c)
		return false
	}
}

// queueResync replaces the oldest queued event with a resync marker. Only the
// latest DOM state matters, so discarding a stale fragment to make room for the
// marker loses nothing the client will not re-fetch.
func (b *Broker) queueResync(c *Connection) {
	if !c.resyncPending.CompareAndSwap(false, true) {
		return
	}

	select {
	case <-c.ch:
	default:
	}

	select {
	case c.ch <- resyncEvent:
	default:
		// Another publisher took the slot; clear the flag so the next drop
		// retries rather than leaving the client without a resync.
		c.resyncPending.Store(false)
	}
}

// deliver evaluates the filter and sends to a snapshot of connections. It runs
// outside b.mu: the caller-supplied filter must never block the whole broker,
// and holding the read lock here would starve UpdateConnectionPage, which sits
// on the /unh and /uih request paths.
func (b *Broker) deliver(targets []*Connection, e Event, filter func(*Connection) bool) {
	for _, c := range targets {
		if filter != nil && !filter(c) {
			continue
		}
		b.send(c, e)
	}
}

// Publish fans an event out to all connected clients.
// Slow clients are skipped (non-blocking send).
func (b *Broker) Publish(e Event) {
	targets := getConnSlice()
	defer putConnSlice(targets)

	b.mu.RLock()
	for _, c := range b.connections {
		*targets = append(*targets, c)
	}
	b.mu.RUnlock()

	b.deliver(*targets, e, nil)
}

// Count returns the number of active subscribers.
func (b *Broker) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.connections)
}

// Stats returns a snapshot of connection count and lifetime publish/drop counts.
func (b *Broker) Stats() Stats {
	return Stats{
		Connections: b.Count(),
		Published:   b.published.Load(),
		Dropped:     b.dropped.Load(),
	}
}

// ConnectionDrops returns how many events were dropped for a connection, and
// whether that connection is still registered.
func (b *Broker) ConnectionDrops(connID uint64) (uint64, bool) {
	b.mu.RLock()
	c, ok := b.connections[connID]
	b.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return c.dropped.Load(), true
}

// PublishToConnection sends an event to one connection. It reports false when
// the connection is unknown or its mailbox is full.
func (b *Broker) PublishToConnection(connectionID uint64, e Event) bool {
	b.mu.RLock()
	connection, ok := b.connections[connectionID]
	b.mu.RUnlock()
	if !ok {
		return false
	}
	return b.send(connection, e)
}

func (b *Broker) PublishToConnections(connectionIDs []uint64, e Event) {
	targets := getConnSlice()
	defer putConnSlice(targets)

	b.mu.RLock()
	for _, id := range connectionIDs {
		if c, ok := b.connections[id]; ok {
			*targets = append(*targets, c)
		}
	}
	b.mu.RUnlock()

	b.deliver(*targets, e, nil)
}

// PublishToUser fans an event out to all connections for the given user.
func (b *Broker) PublishToUser(userID string, e Event) {
	b.PublishToUserWithOptions(userID, e, nil)
}

// PublishToUserWithOptions fans an event out to the given user's connections,
// optionally restricted to active sessions and to connections matching Filter.
func (b *Broker) PublishToUserWithOptions(userID string, e Event, opts *PublishOptions) {
	active, filter := splitOptions(opts)

	targets := getConnSlice()
	defer putConnSlice(targets)

	b.mu.RLock()
	b.collectUserLocked(userID, active, targets)
	b.mu.RUnlock()

	b.deliver(*targets, e, filter)
}

func (b *Broker) PublishToUsers(userIDs []string, e Event) {
	b.PublishToUsersWithOptions(userIDs, e, nil)
}

// PublishToUsersWithOptions fans an event out to multiple users.
func (b *Broker) PublishToUsersWithOptions(userIDs []string, e Event, opts *PublishOptions) {
	active, filter := splitOptions(opts)

	targets := getConnSlice()
	defer putConnSlice(targets)

	seen := make(map[string]struct{}, len(userIDs))

	b.mu.RLock()
	// Walk the requested users rather than every connected user: the cost must
	// scale with len(userIDs), not with how busy the server is.
	for _, userID := range userIDs {
		if _, dup := seen[userID]; dup {
			continue
		}
		seen[userID] = struct{}{}
		b.collectUserLocked(userID, active, targets)
	}
	b.mu.RUnlock()

	b.deliver(*targets, e, filter)
}

// collectUserLocked appends every routable connection of userID to out.
// b.mu must be held (read is enough).
func (b *Broker) collectUserLocked(userID string, active map[string]bool, out *[]*Connection) {
	for sessionID := range b.users[userID] {
		if active != nil && !active[sessionID] {
			continue
		}
		for connID := range b.sessions[sessionID] {
			if c, ok := b.connections[connID]; ok {
				*out = append(*out, c)
			}
		}
	}
}

func splitOptions(opts *PublishOptions) (map[string]bool, func(*Connection) bool) {
	if opts == nil {
		return nil, nil
	}
	return opts.ActiveSessionIDs, opts.Filter
}

// UpdateUserID re-associates all connections matching oldUserID to newUserID.
// Used when a session token rotates but the underlying browser connection stays open.
func (b *Broker) UpdateUserID(oldUserID string, newUserID string) {
	if oldUserID == newUserID {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	sessions, ok := b.users[oldUserID]
	if !ok {
		return
	}

	if b.users[newUserID] == nil {
		b.users[newUserID] = make(map[string]bool, len(sessions))
	}
	for sessionID := range sessions {
		b.users[newUserID][sessionID] = true
		for connID := range b.sessions[sessionID] {
			c, found := b.connections[connID]
			if !found {
				continue
			}
			ident := c.ident.Load()
			c.ident.Store(&identity{sessionID: ident.sessionID, userID: newUserID})
		}
	}
	delete(b.users, oldUserID)
}

func (b *Broker) removeConnection(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	conn, ok := b.connections[id]
	if !ok {
		return
	}

	delete(b.connections, id)

	ident := conn.ident.Load()
	sessionConns := b.sessions[ident.sessionID]
	delete(sessionConns, id)

	// Only drop the session from the user index once no connection is left on
	// it. Other tabs may share the same session ID and must stay routable.
	if len(sessionConns) == 0 {
		delete(b.sessions, ident.sessionID)
		delete(b.users[ident.userID], ident.sessionID)
		if len(b.users[ident.userID]) == 0 {
			delete(b.users, ident.userID)
		}
	}

	// The mailbox is deliberately left open: publishers send to it without
	// holding b.mu, and closing it here would race them into a panic. Readers
	// already stop on context cancellation, and the channel is garbage
	// collected with the connection.
}
