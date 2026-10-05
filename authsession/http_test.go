package authsession

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"suppa-ahg-stack/common-golang/authapp"
)

func TestRequireAuthRotatesCookieAndContext(t *testing.T) {
	m := testManager(t, func(context.Context, string, string) (*authapp.SessionValidateResponse, error) {
		return &authapp.SessionValidateResponse{
			User:         &authapp.Identity{ID: 1},
			SessionToken: "rotated",
			RefreshToken: "refresh",
		}, nil
	}, nil, ReturnRoleError)
	options := HTTPOptions{Manager: m, SessionName: "session", SessionDomain: ".example.test", SecureCookies: true}
	called := false
	handler := RequireAuth(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = UserFromContext(r.Context()).ID == 1
		cookie, err := r.Cookie("session")
		if err != nil || cookie.Value != "rotated" {
			t.Fatalf("downstream session cookie = %+v, %v", cookie, err)
		}
	}), options, false)
	req := httptest.NewRequest(http.MethodGet, "https://app.example.test/private", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "old"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if !called {
		t.Fatal("authenticated handler was not called")
	}
	if got := recorder.Header().Values("Set-Cookie"); len(got) != 2 || !strings.Contains(got[0], "Secure") {
		t.Fatalf("Set-Cookie = %v", got)
	}
}

func TestEnsureSessionKnownDoesNotReplaceCookieOnDependencyFailure(t *testing.T) {
	m := testManager(t, func(context.Context, string, string) (*authapp.SessionValidateResponse, error) {
		return nil, authapp.ErrUnavailable
	}, nil, ReturnRoleError)
	options := HTTPOptions{Manager: m, SessionName: "session"}
	handler := EnsureSessionKnown(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("downstream handler must not run")
	}), options)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "possibly-valid"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Set-Cookie"); got != "" {
		t.Fatalf("dependency failure replaced cookie: %q", got)
	}
}

func TestEnsureSessionKnownDoesNotMisclassifyDependencyFailure(t *testing.T) {
	m := testManager(t, func(context.Context, string, string) (*authapp.SessionValidateResponse, error) {
		return nil, authapp.ErrUnavailable
	}, nil, ReturnRoleError)
	options := HTTPOptions{
		Manager:                  m,
		SessionName:              "session",
		EnsureKnownFailureStatus: http.StatusUnauthorized,
	}
	handler := EnsureSessionKnown(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("downstream handler must not run")
	}), options)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "possibly-valid"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Set-Cookie"); got != "" {
		t.Fatalf("dependency failure replaced cookie: %q", got)
	}
}

func TestEnsureSessionCookieCreatesSecureAnonymousSession(t *testing.T) {
	m := NewManager(validatorFunc(func(context.Context, string, string) (*authapp.SessionValidateResponse, error) {
		return &authapp.SessionValidateResponse{}, nil
	}), nil, Config{MaxAge: time.Hour}, nil)
	defer m.Stop()
	options := HTTPOptions{Manager: m, SessionName: "session", SecureCookies: true}
	handler := EnsureSessionCookie(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("session"); err != nil || !m.HasSession(cookie.Value) {
			t.Fatalf("anonymous request cookie = %+v, %v", cookie, err)
		}
	}), options)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "https://app.test/", nil))
	if !strings.Contains(recorder.Header().Get("Set-Cookie"), "Secure") {
		t.Fatalf("Set-Cookie = %q", recorder.Header().Get("Set-Cookie"))
	}
}

func TestAuthenticationRedirectPolicy(t *testing.T) {
	for _, tc := range []struct {
		method, accept string
		status         int
	}{{"GET", "text/html", 303}, {"GET", "application/json", 401}, {"POST", "", 401}} {
		rr := httptest.NewRecorder()
		r := httptest.NewRequest(tc.method, "https://task.example/private", nil)
		r.Header.Set("Accept", tc.accept)
		RedirectToAuthApp(rr, r, HTTPOptions{PublicBaseURL: "https://task.example", AuthPublicURL: "https://auth.task.example"})
		if rr.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.accept, rr.Code)
		}
		if tc.status == 303 && !strings.HasPrefix(rr.Header().Get("Location"), "https://auth.task.example/login/") {
			t.Fatal(rr.Header())
		}
	}
}
