//go:build darwin || linux

package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Run the checked-in command as a separate process against compiled normal Wasm.
// Its two cancellation orderings and committed-reply barrier do not pick winners
// by elapsed time. Deadlines are watchdogs, not expected outcomes.
func TestSDKExamples(t *testing.T) {
	credential := token(t)
	root, _, _, runtime := staticSetup(t, []object{staticKey(credential, "examples", "developer")})
	defer runtime.stop()
	binary := filepath.Join(t.TempDir(), "mutex")
	build := exec.Command(filepath.Join(root, "bin/go"), "build", "-race", "-o", binary, "./examples/mutex")
	build.Dir = root
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build example: %v\n%s", err, b)
	}
	for _, mode := range []string{"lifecycle", "cancel", "recover"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "-mode", mode)
			cmd.Env = append(os.Environ(), "PINTLE_ENDPOINT="+runtime.base, "PINTLE_TOKEN="+credential, "PINTLE_NAMESPACE=test")
			b, err := cmd.CombinedOutput()
			if strings.Contains(string(b), credential) {
				t.Fatal("example printed credential")
			}
			if err != nil {
				t.Fatalf("example: %v\n%s", err, b)
			}
			if !strings.Contains(string(b), "PASS "+mode) {
				t.Fatalf("missing completion: %s", b)
			}
			t.Log(strings.TrimSpace(string(b)))
		})
	}
}
