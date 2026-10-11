package ui

import (
	"context"
	"strings"
	"testing"
)

func TestSSEAvailabilityDialogBlocksBeforeJavaScriptAndEscapesCopy(t *testing.T) {
	html := renderToString(t, context.Background(), SSEAvailabilityDialog(SSEAvailabilityDialogProps{
		ID: "sse-blocker", ConnectingTitle: "Connecting <script>", LoginLabel: "Sign in & resume",
	}))
	for _, required := range []string{`id="sse-blocker"`, ` open`, `aria-modal="true"`, `data-sse-title`, `data-sse-message`, `data-sse-login hidden`, `aria-live="polite"`, `Connecting &lt;script&gt;`, `Sign in &amp; resume`} {
		if !strings.Contains(html, required) {
			t.Errorf("missing %q in %s", required, html)
		}
	}
	for _, forbidden := range []string{`method="dialog"`, `onclick=`, `<script>`} {
		if strings.Contains(html, forbidden) {
			t.Errorf("unexpected %q", forbidden)
		}
	}
}
