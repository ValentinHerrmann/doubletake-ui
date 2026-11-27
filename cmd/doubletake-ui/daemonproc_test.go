package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHasAmbientCaps(t *testing.T) {
	if !hasAmbientCaps("Name:\tnode\nCapAmb:\t0000000000000400\n") {
		t.Fatal("expected ambient caps")
	}
	if hasAmbientCaps("CapAmb:\t0000000000000000\n") {
		t.Fatal("expected no ambient caps")
	}
}

func TestSessionEnvFillsDefaults(t *testing.T) {
	env := sessionEnv([]string{"HOME=/home/x", "XDG_RUNTIME_DIR=/nonexistent"})
	joined := strings.Join(env, "\n")
	for _, want := range []string{"HOME=/home/x", "DBUS_SESSION_BUS_ADDRESS=unix:path=/nonexistent/bus", "DISPLAY=:0"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("env missing %q: %v", want, env)
		}
	}
	kept := sessionEnv([]string{"XDG_RUNTIME_DIR=/r", "WAYLAND_DISPLAY=wayland-1"})
	if strings.Contains(strings.Join(kept, "\n"), "DISPLAY=:0") {
		t.Fatalf("DISPLAY set despite WAYLAND_DISPLAY: %v", kept)
	}
}

func TestDaemonLogPathFollowsXDGStateHome(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/state")
	if got, want := daemonLogPath(), "/state/doubletake/daemon.log"; got != want {
		t.Fatalf("daemonLogPath() = %q, want %q", got, want)
	}
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", "/home/x")
	if got, want := daemonLogPath(), "/home/x/.local/state/doubletake/daemon.log"; got != want {
		t.Fatalf("daemonLogPath() = %q, want %q", got, want)
	}
}

func TestOpenDaemonLogAppendsAndRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doubletake", "daemon.log")
	write := func(text string) {
		t.Helper()
		f, err := openDaemonLog(path)
		if err != nil {
			t.Fatalf("openDaemonLog: %v", err)
		}
		if _, err := f.WriteString(text); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	write("one\n")
	write("two\n")
	if data, _ := os.ReadFile(path); string(data) != "one\ntwo\n" {
		t.Fatalf("log = %q, want both runs appended", data)
	}

	if err := os.WriteFile(path, make([]byte, daemonLogMaxBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	write("fresh\n")
	if data, _ := os.ReadFile(path); string(data) != "fresh\n" {
		t.Fatalf("log after rotation = %q, want a fresh file", data)
	}
	if info, err := os.Stat(path + ".1"); err != nil || info.Size() != daemonLogMaxBytes+1 {
		t.Fatalf("rotated log: %v, %v", info, err)
	}
}
