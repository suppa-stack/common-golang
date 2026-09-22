package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/invopop/ctxi18n"
	"suppa-ahg-stack/common-golang/serverutil"
	"suppa-ahg-stack/common-golang/sse"
)

// SseConnectionIDHeader is the header used by the client to report its current
// SSE connection identifier.
const SseConnectionIDHeader = "X-SSE-Connection-ID"

// MaxFragmentSize is the maximum number of bytes a rendered fragment may
// occupy in memory before being published via SSE. Fragments that exceed
// this limit are dropped with an error log instead of being broadcast.
const MaxFragmentSize = 8 * 1024 * 1024 // 8 MiB

// FragmentSSEPayload is the wire payload for a fragment update.
type FragmentSSEPayload struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Selector string `json:"selector"`
	HTML     string `json:"html"`
	Swap     string `json:"swap"`
	Path     string `json:"path,omitempty"`
}

// DataSSEPayload is the wire payload for a business/UI event.
type DataSSEPayload struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	FragmentID string `json:"fragment_id,omitempty"`
	Data       any    `json:"data"`
}

// ConfigWithSession exposes session and locale configuration.
type ConfigWithSession interface {
	GetLangCookieName() string
	GetAppDefaultLang() string
	GetSessionName() string
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) getConfig() ConfigWithSession {
	cfg, ok := any(a.Config).(ConfigWithSession)
	if !ok {
		panic(fmt.Sprintf("Config type %T does not implement ConfigWithSession", a.Config))
	}
	return cfg
}

// targetUserID returns the user-level routing key for a request.
// Authenticated users are identified by their application user ID;
// anonymous users fall back to their session cookie value.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) targetUserID(r *http.Request) string {
	if a.GetUserID != nil {
		if userID := a.GetUserID(r); userID != "" {
			return userID
		}
	}
	cfg := a.getConfig()
	cookie, err := r.Cookie(cfg.GetSessionName())
	if err == nil && cookie != nil {
		return cookie.Value
	}
	return ""
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) activeSessionIDs(userID string) map[string]bool {
	if a.GetActiveSessionIDsForUser != nil {
		return a.GetActiveSessionIDsForUser(userID)
	}
	return nil
}

// UpdateConnectionPageFromRequest updates the page context for the SSE
// connection identified by the X-SSE-Connection-ID header, if present and valid.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) UpdateConnectionPageFromRequest(r *http.Request, path string) {
	header := r.Header.Get(SseConnectionIDHeader)
	if header == "" {
		return
	}

	connID, err := strconv.ParseUint(header, 10, 64)
	if err != nil {
		return
	}

	selectors, fragmentIDs := a.FragmentsForPage(path)
	a.DomUpdateBroker.UpdateConnectionPage(connID, path, selectors, fragmentIDs)
}

// FragmentsForPage returns the selectors and fragment IDs present on the given page.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) FragmentsForPage(path string) (selectors, fragmentIDs map[string]bool) {
	selectors = make(map[string]bool)
	fragmentIDs = make(map[string]bool)

	routeKey, _, ok := MatchRoute(a.Routes, path)
	if ok && a.FragmentPlanner != nil {
		for _, fragment := range a.FragmentPlanner.AllFragments(routeKey) {
			selectors[fragment.Selector] = true
			fragmentIDs[string(fragment.ID)] = true
		}
	} else {
		selectors["#page-content"] = true
	}

	for _, gf := range a.GlobalFragments {
		selectors[gf.Selector] = true
		fragmentIDs[string(gf.ID)] = true
	}

	return selectors, fragmentIDs
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) publishEvent(userID string, event sse.Event, active map[string]bool, filter func(*sse.Connection) bool) {
	opts := &sse.PublishOptions{
		ActiveSessionIDs: active,
		Filter:           filter,
	}
	a.DomUpdateBroker.PublishToUserWithOptions(userID, event, opts)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishFragmentForPath(path string, selector string, r *http.Request) {
	cfg := a.getConfig()
	route, ok := a.Routes[path]
	if !ok {
		a.Logger.Error(fmt.Sprintf("publishFragmentForPath: unknown path %s", path))
		return
	}

	targetSelector := selector
	if targetSelector == "" {
		targetSelector = route.TargetSelector
	}

	langCookie, err := r.Cookie(cfg.GetLangCookieName())
	if !errors.Is(err, http.ErrNoCookie) && err != nil {
		a.Logger.Error(fmt.Sprintf("Error while retrieving lang cookie with error: %v", err))
		return
	}

	locale := cfg.GetAppDefaultLang()
	if langCookie != nil {
		locale = langCookie.Value
	}

	ctx, err := ctxi18n.WithLocale(r.Context(), locale)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Error while setting locale: %v", err))
		return
	}
	r = r.WithContext(ctx)
	var buf bytes.Buffer
	if err := route.PageContentFunc().Render(ctx, &buf); err != nil {
		a.Logger.Error(fmt.Sprintf("Failed to render fragment for %s: %v", path, err))
		return
	}
	if buf.Len() > MaxFragmentSize {
		a.Logger.Error(fmt.Sprintf("Fragment for %s exceeds %d MiB limit (%d bytes); dropped", path, MaxFragmentSize/(1024*1024), buf.Len()))
		return
	}

	a.PublishDomUpdate(path, targetSelector, buf.String(), r)

	for sel, component := range a.PageComponent[path] {
		if sel == targetSelector || !component.IsLayoutComponent() {
			continue
		}
		a.PublishFragment(path, sel, r)
	}
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishFragment(path string, selector string, r *http.Request) {
	cfg := a.getConfig()
	component, ok := a.PageComponent[path][selector]
	if !ok {
		a.Logger.Error(fmt.Sprintf("publishFragment: unknown for path %s and selector %s", path, selector))
		return
	}

	targetSelector := selector
	if targetSelector == "" {
		targetSelector = component.GetTargetSelector()
	}

	langCookie, err := r.Cookie(cfg.GetLangCookieName())
	if !errors.Is(err, http.ErrNoCookie) && err != nil {
		a.Logger.Error(fmt.Sprintf("Error while retrieving lang cookie with error: %v", err))
		return
	}

	locale := cfg.GetAppDefaultLang()
	if langCookie != nil {
		locale = langCookie.Value
	}

	ctx, err := ctxi18n.WithLocale(r.Context(), locale)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Error while setting locale: %v", err))
		return
	}
	r = r.WithContext(ctx)
	var buf bytes.Buffer
	if err := component.Render(r, ctx, &buf); err != nil {
		a.Logger.Error("Failed to render fragment for %s: %v", path, err)
		return
	}
	if buf.Len() > MaxFragmentSize {
		a.Logger.Error(fmt.Sprintf("Fragment for %s/%s exceeds %d MiB limit (%d bytes); dropped", path, selector, MaxFragmentSize/(1024*1024), buf.Len()))
		return
	}

	a.PublishDomUpdate(path, targetSelector, buf.String(), r)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishToast(message string, toastType string, duration int, r *http.Request) {
	userID := a.targetUserID(r)
	if userID == "" {
		return
	}

	payload := DataSSEPayload{
		Kind:       "data",
		Name:       "ui.toast",
		FragmentID: "toast-container",
		Data: map[string]any{
			"message":  message,
			"type":     toastType,
			"duration": duration,
			"scope":    "global",
		},
	}

	eventData, err := json.Marshal(payload)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Failed to marshal toast event: %v", err))
		return
	}

	active := a.activeSessionIDs(userID)
	a.publishEvent(userID, sse.Event{Type: "app-event", Data: eventData}, active, func(conn *sse.Connection) bool {
		if !conn.HasPageContext() {
			return true
		}
		_, _, fragmentIDs := conn.PageContext()
		return fragmentIDs[payload.FragmentID]
	})
}

// PublishDomUpdate sends a single fragment SSE event to the current user's stream.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishDomUpdate(rawPath, selector, html string, r *http.Request) bool {
	userID := a.targetUserID(r)
	if userID == "" {
		return false
	}

	payload := FragmentSSEPayload{
		Kind:     "fragment",
		Name:     "ui.fragment",
		Selector: selector,
		HTML:     html,
		Path:     rawPath,
		Swap:     "morph",
	}

	eventData, err := json.Marshal(payload)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Failed to marshal nav event: %v", err))
		return false
	}

	active := a.activeSessionIDs(userID)
	// The page must match, not just the selector: #page-content exists on every
	// page, so filtering on the selector alone pushes this fragment into the
	// user's other tabs and makes the client rewrite their URL to rawPath.
	targetPage := normalizePagePath(rawPath)
	a.publishEvent(userID, sse.Event{Type: "app-event", Data: eventData}, active, func(conn *sse.Connection) bool {
		if !conn.HasPageContext() {
			return true
		}
		page, selectors, _ := conn.PageContext()
		return selectors[selector] && normalizePagePath(page) == targetPage
	})
	return true
}

// normalizePagePath reduces a path to the form used to decide whether a
// connection is on the page a fragment was rendered for. The query string is
// dropped and the trailing slash is canonical, because a connection's stored
// page comes from several sources that disagree on both: the Referer at
// connect time, the raw payload.Path of /uih, and the already-normalized path
// used to publish. Dynamic segments are preserved, so two tabs on different
// records of the same route stay distinct.
func normalizePagePath(path string) string {
	if u, err := url.Parse(path); err == nil {
		path = u.Path
	}
	if path == "" {
		return "/"
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

// PublishModalToast sends a toast event scoped to a modal container.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishModalToast(message, toastType, duration, modalID string, r *http.Request) {
	userID := a.targetUserID(r)
	if userID == "" {
		return
	}

	payload := DataSSEPayload{
		Kind: "data",
		Name: "ui.toast",
		Data: map[string]any{
			"message":  message,
			"type":     toastType,
			"duration": duration,
			"scope":    "modal",
			"modal_id": modalID,
		},
	}

	eventData, err := json.Marshal(payload)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Failed to marshal modal toast event: %v", err))
		return
	}

	active := a.activeSessionIDs(userID)
	a.publishEvent(userID, sse.Event{Type: "app-event", Data: eventData}, active, nil)
}

// PublishFragmentScope renders and publishes a planned set of fragments for rawPath.
// It uses the configured FragmentPlanner; if none is configured, it falls back to
// publishing the route content only.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishFragmentScope(rawPath string, roots []FragmentID, r *http.Request) bool {
	if a.FragmentPlanner == nil {
		a.PublishFragmentForPath(rawPath, "", r)
		return true
	}

	routeKey, route, ok := MatchRoute(a.Routes, rawPath)
	if !ok {
		a.Logger.Error(fmt.Sprintf("PublishFragmentScope: unknown path %s", rawPath))
		return false
	}

	r, ctx, err := a.buildFragmentRequest(rawPath, r)
	if err != nil {
		a.Logger.Error(fmt.Sprintf("Error while setting locale: %v", err))
		return false
	}

	for _, fragment := range a.FragmentPlanner.Plan(routeKey, roots) {
		switch fragment.Kind {
		case FragmentRouteContent:
			if !a.publishRouteContent(rawPath, route, fragment.Selector, r, ctx) {
				return false
			}
		case FragmentPageComponent:
			if !a.publishPageComponent(rawPath, routeKey, fragment.Selector, r, ctx) {
				return false
			}
		case FragmentCustom:
			if !a.publishCustomFragment(rawPath, fragment, r, ctx) {
				return false
			}
		default:
			a.Logger.Error(fmt.Sprintf("PublishFragmentScope: unknown fragment kind for %s", fragment.ID))
			return false
		}
	}

	return true
}

// PublishFragmentForPathWithPlanner publishes the fragment for rawPath and selector
// using the FragmentPlanner. If no planner is configured, it falls back to the
// legacy PublishFragmentForPath behavior.
func (a *App[TConfig, TQueries, TSessionService, TSseNames]) PublishFragmentForPathWithPlanner(rawPath, selector string, r *http.Request) {
	if a.FragmentPlanner == nil {
		a.PublishFragmentForPath(rawPath, selector, r)
		return
	}

	routeKey, route, ok := MatchRoute(a.Routes, rawPath)
	if !ok {
		a.Logger.Error(fmt.Sprintf("PublishFragmentForPathWithPlanner: unknown path %s", rawPath))
		return
	}

	targetSelector := selector
	if targetSelector == "" {
		targetSelector = route.TargetSelector
	}

	if targetSelector == route.TargetSelector {
		a.PublishFragmentScope(rawPath, RootsForPolicy(RefreshPageOnly), r)
		return
	}

	fragment, ok := a.FragmentPlanner.FindBySelector(routeKey, targetSelector)
	if !ok {
		a.Logger.Error(fmt.Sprintf("PublishFragmentForPathWithPlanner: unknown selector %s for path %s", targetSelector, rawPath))
		return
	}

	a.PublishFragmentScope(rawPath, []FragmentID{fragment.ID}, r)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) publishRouteContent(rawPath string, route serverutil.PageRoute, selector string, r *http.Request, ctx context.Context) bool {
	var buf bytes.Buffer
	if err := route.PageContentFunc().Render(ctx, &buf); err != nil {
		a.Logger.Error(fmt.Sprintf("Failed to render fragment for %s: %v", rawPath, err))
		return false
	}
	if buf.Len() > MaxFragmentSize {
		a.Logger.Error(fmt.Sprintf("Fragment for %s exceeds %d MiB limit (%d bytes); dropped", rawPath, MaxFragmentSize/(1024*1024), buf.Len()))
		return false
	}

	return a.PublishDomUpdate(rawPath, selector, buf.String(), r)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) publishPageComponent(rawPath, routeKey, selector string, r *http.Request, ctx context.Context) bool {
	components, ok := a.PageComponent[routeKey]
	if !ok {
		return false
	}
	component, ok := components[selector]
	if !ok {
		a.Logger.Error(fmt.Sprintf("publishPageComponent: unknown for path %s and selector %s", routeKey, selector))
		return false
	}

	var buf bytes.Buffer
	if err := component.Render(r, ctx, &buf); err != nil {
		a.Logger.Error(fmt.Sprintf("publishPageComponent: failed to render %s: %v", selector, err))
		return false
	}
	if buf.Len() > MaxFragmentSize {
		a.Logger.Error(fmt.Sprintf("Component %s/%s exceeds %d MiB limit (%d bytes); dropped", rawPath, selector, MaxFragmentSize/(1024*1024), buf.Len()))
		return false
	}

	return a.PublishDomUpdate(rawPath, selector, buf.String(), r)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) publishCustomFragment(rawPath string, fragment FragmentSpec, r *http.Request, ctx context.Context) bool {
	if fragment.RenderFunc == nil {
		a.Logger.Error(fmt.Sprintf("publishCustomFragment: missing render func for %s", fragment.ID))
		return false
	}

	var buf bytes.Buffer
	if err := fragment.RenderFunc(r, ctx, &buf); err != nil {
		a.Logger.Error(fmt.Sprintf("publishCustomFragment: failed to render %s: %v", fragment.ID, err))
		return false
	}
	if buf.Len() > MaxFragmentSize {
		a.Logger.Error(fmt.Sprintf("Custom fragment %s exceeds %d MiB limit (%d bytes); dropped", fragment.ID, MaxFragmentSize/(1024*1024), buf.Len()))
		return false
	}

	return a.PublishDomUpdate(rawPath, fragment.Selector, buf.String(), r)
}

func (a *App[TConfig, TQueries, TSessionService, TSseNames]) buildFragmentRequest(rawPath string, r *http.Request) (*http.Request, context.Context, error) {
	cfg := a.getConfig()
	locale := cfg.GetAppDefaultLang()
	if langCookie, err := r.Cookie(cfg.GetLangCookieName()); err == nil && langCookie != nil {
		locale = langCookie.Value
	}

	ctx, err := ctxi18n.WithLocale(r.Context(), locale)
	if err != nil {
		return nil, nil, err
	}

	if TargetPathFromContext(ctx) == "" {
		ctx = WithTargetPath(ctx, rawPath)
	}
	return r.WithContext(ctx), ctx, nil
}
