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
	root, _, _, runtime := staticSetup(t, []object{staticKey(credential, "examples", "developer")}, "PINTLE_COOLDOWN_ENABLED:true")
	defer runtime.stop()
	binary := filepath.Join(t.TempDir(), "mutex")
	build := exec.Command(filepath.Join(root, "bin/go"), "build", "-race", "-o", binary, "./examples/mutex")
	build.Dir = root
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build example: %v\n%s", err, b)
	}
	for _, mode := range []string{"lifecycle", "cancel", "recover", "with-lease", "with-fencing"} {
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
	for _, example := range []string{"semaphore", "cooldown"} {
		t.Run(example, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), example)
			build := exec.Command(filepath.Join(root, "bin/go"), "build", "-race", "-o", binary, "./examples/"+example)
			build.Dir = root
			if b, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build %s: %v\n%s", example, err, b)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary)
			cmd.Env = append(os.Environ(), "PINTLE_ENDPOINT="+runtime.base, "PINTLE_TOKEN="+credential, "PINTLE_NAMESPACE=test")
			b, err := cmd.CombinedOutput()
			if strings.Contains(string(b), credential) {
				t.Fatal("example leaked credential")
			}
			if err != nil {
				t.Fatalf("example failed: %v\n%s", err, b)
			}
			switch example {
			case "semaphore":
				if !strings.Contains(string(b), "PASS semaphore") {
					t.Fatalf("missing completion: %s", b)
				}
			case "cooldown":
				lines := strings.Split(strings.TrimSpace(string(b)), "\n")
				if len(lines) != 2 || lines[0] != "worker A published revision 2" {
					t.Fatalf("unexpected refresh output: %s", b)
				}
				when, ok := strings.CutPrefix(lines[1], "worker B can try again at ")
				if _, err := time.Parse(time.RFC3339Nano, when); !ok || err != nil {
					t.Fatalf("missing retry time: %s", b)
				}
			}
			t.Log(strings.TrimSpace(string(b)))
		})
	}
}
