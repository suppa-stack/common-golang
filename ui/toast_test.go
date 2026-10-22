package ui

import (
	"context"
	"strings"
	"testing"
)

func TestToast(t *testing.T) {
	html := renderToString(t, context.Background(), Toast())
	for _, expected := range []string{
		`id="toast-container"`,
		`@mouseenter="store.pauseTimer(toast.id)"`,
		`@mouseleave="store.resumeTimer(toast.id)"`,
		`animation: toast-shrink`,
	} {
		if !strings.Contains(html, expected) {
			t.Errorf("toast is missing %q", expected)
		}
	}
}

// toast-error-rendering:c1,c2 — the error variant renders an identifiable
// icon with an alert semantic, and every variant keeps complete Tailwind
// class names in the markup instead of a runtime concatenation.
func TestToastErrorPresentation(t *testing.T) {
	html := renderToString(t, context.Background(), Toast())
	for _, expected := range []string{
		`'alert-error'`,
		`'alert-success'`,
		`'alert-warning'`,
		`'alert-info'`,
		`:role="toast.type === 'error' ? 'alert' : 'status'"`,
		`:aria-live="toast.type === 'error' ? 'assertive' : 'polite'"`,
		`x-show="toast.type === 'error'"`,
		`aria-hidden="true"`,
		`<circle cx="12" cy="12" r="10">`,
	} {
		if !strings.Contains(html, expected) {
			t.Errorf("toast is missing %q", expected)
		}
	}
	if strings.Contains(html, `'alert-' + toast.type`) {
		t.Error("toast must keep complete class names for Tailwind detection, not a runtime concatenation")
	}
}
