package authsession

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type contextKey struct{}

func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(contextKey{}).(*User)
	return user
}

func ContextWithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, contextKey{}, user)
}

type HTTPLogger interface {
	Debug(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

type ConnectionSessionUpdater interface {
	UpdateConnectionSessionID(connectionID uint64, sessionID string) bool
}

type HTTPOptions struct {
	Manager          *Manager
	SessionName      string
	SessionDomain    string
	SecureCookies    bool
	PublicBaseURL    string
	ServerAddress    string
	ServerPort       string
	AuthPublicURL    string
	AuthInternalURL  string
	Development      bool
	Logger           HTTPLogger
	ConnectionBroker ConnectionSessionUpdater
	HandleError      func(http.ResponseWriter, *http.Request, int)
	// Deprecated: dependency failures always return 503. Kept for source compatibility.
	EnsureKnownFailureStatus int
}

func (o HTTPOptions) handleError(w http.ResponseWriter, r *http.Request, status int) {
	if o.HandleError != nil {
		o.HandleError(w, r, status)
		return
	}
	http.Error(w, http.StatusText(status), status)
}

func RequireAuth(next http.Handler, options HTTPOptions, fresh bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, refresh, ok := requestTokens(r, options.SessionName)
		if !ok {
			options.logDebug("RequireAuth: no session cookie", "method", r.Method, "path", r.URL.Path)
			RedirectToAuthApp(w, r, options)
			return
		}
		validate := options.Manager.Validate
		if fresh {
			validate = options.Manager.ValidateFresh
		}
		user, newSession, newRefresh, err := validate(r.Context(), session, refresh)
		if err != nil {
			options.logError("RequireAuth: session validation failed", "error", err)
			options.handleError(w, r, http.StatusServiceUnavailable)
			return
		}
		if user == nil {
			RedirectToAuthApp(w, r, options)
			return
		}
		SetRotatedSessionCookies(w, r, options, newSession, newRefresh)
		next.ServeHTTP(w, r.WithContext(ContextWithUser(r.Context(), user)))
	})
}

func SetUserFromSession(next http.Handler, options HTTPOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, refresh, ok := requestTokens(r, options.SessionName)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		user, newSession, newRefresh, err := options.Manager.Validate(r.Context(), session, refresh)
		if err != nil || user == nil {
			next.ServeHTTP(w, r)
			return
		}
		SetRotatedSessionCookies(w, r, options, newSession, newRefresh)
		next.ServeHTTP(w, r.WithContext(ContextWithUser(r.Context(), user)))
	})
}

func EnsureSessionCookie(next http.Handler, options HTTPOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := r.Cookie(options.SessionName)
		if errors.Is(err, http.ErrNoCookie) {
			token, createErr := options.Manager.CreateAnonymous()
			if createErr != nil {
				options.logError("EnsureSessionCookie: create anonymous session", "error", createErr)
				options.handleError(w, r, http.StatusInternalServerError)
				return
			}
			setCookie(w, options.SessionName, token, options)
			replaceRequestCookie(r, options.SessionName, token)
		} else if err != nil {
			options.handleError(w, r, http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func EnsureSessionKnown(next http.Handler, options HTTPOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, refresh, ok := requestTokens(r, options.SessionName)
		if !ok {
			options.handleError(w, r, http.StatusInternalServerError)
			return
		}
		newSession, newRefresh, err := options.Manager.EnsureKnown(r.Context(), session, refresh)
		if err != nil {
			options.logError("EnsureSessionKnown: validate session", "error", err)
			options.handleError(w, r, http.StatusServiceUnavailable)
			return
		}
		SetRotatedSessionCookies(w, r, options, newSession, newRefresh)
		next.ServeHTTP(w, r)
	})
}

func SetRotatedSessionCookies(w http.ResponseWriter, r *http.Request, options HTTPOptions, session, refresh string) {
	if session == "" {
		return
	}
	if options.ConnectionBroker != nil {
		if id, err := strconv.ParseUint(r.Header.Get("X-SSE-Connection-ID"), 10, 64); err == nil {
			options.ConnectionBroker.UpdateConnectionSessionID(id, session)
		}
	}
	setCookie(w, options.SessionName, session, options)
	replaceRequestCookie(r, options.SessionName, session)
	if refresh != "" {
		setCookie(w, options.SessionName+"_refresh", refresh, options)
		replaceRequestCookie(r, options.SessionName+"_refresh", refresh)
	}
}

func RedirectToAuthApp(w http.ResponseWriter, r *http.Request, options HTTPOptions) {
	if r.Method != http.MethodGet || strings.Contains(r.Header.Get("Accept"), "application/json") || r.Header.Get("X-Requested-With") == "XMLHttpRequest" {
		options.handleError(w, r, http.StatusUnauthorized)
		return
	}
	publicBaseURL := strings.TrimRight(options.PublicBaseURL, "/")
	if publicBaseURL == "" {
		scheme := "https"
		if options.Development {
			scheme = "http"
		}
		host := options.ServerAddress
		if options.ServerPort != "" {
			host = net.JoinHostPort(host, options.ServerPort)
		}
		publicBaseURL = fmt.Sprintf("%s://%s", scheme, host)
	}
	authURL := strings.TrimRight(options.AuthPublicURL, "/")
	if authURL == "" {
		authURL = strings.TrimRight(options.AuthInternalURL, "/")
	}
	target := url.QueryEscape(publicBaseURL + r.URL.RequestURI())
	http.Redirect(w, r, fmt.Sprintf("%s/login/?redirect=%s", authURL, target), http.StatusSeeOther)
}

func requestTokens(r *http.Request, sessionName string) (string, string, bool) {
	cookie, err := r.Cookie(sessionName)
	if err != nil || cookie.Value == "" {
		return "", "", false
	}
	refresh := ""
	if cookie, err := r.Cookie(sessionName + "_refresh"); err == nil {
		refresh = cookie.Value
	}
	return cookie.Value, refresh, true
}

func setCookie(w http.ResponseWriter, name, value string, options HTTPOptions) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		Domain:   options.SessionDomain,
		HttpOnly: true,
		Secure:   options.SecureCookies,
		SameSite: http.SameSiteLaxMode,
	})
}

func replaceRequestCookie(r *http.Request, name, value string) {
	cookies := r.Cookies()
	r.Header.Del("Cookie")
	replaced := false
	for _, cookie := range cookies {
		if cookie.Name == name {
			if replaced {
				continue
			}
			cookie.Value = value
			replaced = true
		}
		r.AddCookie(cookie)
	}
	if !replaced {
		r.AddCookie(&http.Cookie{Name: name, Value: value})
	}
}

func (o HTTPOptions) logDebug(message string, values ...any) {
	if o.Logger != nil {
		o.Logger.Debug(message, values...)
	}
}

func (o HTTPOptions) logError(message string, values ...any) {
	if o.Logger != nil {
		o.Logger.Error(message, values...)
	}
}
