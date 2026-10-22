package ui

import (
	"context"
	"strings"
	"testing"
)

func TestModalToastContainer(t *testing.T) {
	html := renderToString(t, context.Background(), ModalToastContainer("application-roles"))
	for _, expected := range []string{
		`id="toast-application-roles"`,
		`data-modal-id="application-roles"`,
		`x-data="modalToastContainer"`,
		`x-for="toast in toasts"`,
		`store.removeToast(toast.id)`,
		`animation: toast-shrink`,
	} {
		if !strings.Contains(html, expected) {
			t.Errorf("modal toast is missing %q: %s", expected, html)
		}
	}
}

// toast-error-rendering:c1,c2 — the modal-scoped container renders the same
// error icon, alert semantic and complete class names as the global toast.
func TestModalToastContainerErrorPresentation(t *testing.T) {
	html := renderToString(t, context.Background(), ModalToastContainer("application-roles"))
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
			t.Errorf("modal toast is missing %q", expected)
		}
	}
	if strings.Contains(html, `'alert-' + toast.type`) {
		t.Error("modal toast must keep complete class names for Tailwind detection, not a runtime concatenation")
	}
}
