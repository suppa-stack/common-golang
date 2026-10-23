package ui

import (
	"context"
	"html"
	"strings"
	"testing"

	"github.com/a-h/templ"
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

// Both entry points must expose the same presentation and accessibility
// contract; these literal variants also keep Tailwind's source scan complete.
func TestToastVariantsAndAccessibility(t *testing.T) {
	for name, component := range map[string]templ.Component{
		"global": Toast(), "modal": ModalToastContainer("example"),
	} {
		t.Run(name, func(t *testing.T) {
			rendered := html.UnescapeString(renderToString(t, context.Background(), component))
			for _, expected := range []string{
				"'alert-error'", "'alert-success'", "'alert-warning'", "'alert-debug'", "'alert-info'",
				`:role="toast.type === 'error' ? 'alert' : 'status'"`,
				`:aria-live="toast.type === 'error' ? 'assertive' : 'polite'"`,
				`x-show="toast.type === 'error'" aria-hidden="true"`,
			} {
				if !strings.Contains(rendered, expected) {
					t.Errorf("missing %q", expected)
				}
			}
		})
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
