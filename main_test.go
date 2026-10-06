package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseBitrate(t *testing.T) {
	for in, want := range map[string]int{"128K": 128000, "2m": 2000000, "900": 900} {
		got, err := parseBitrate(in)
		if err != nil || got != want {
			t.Fatalf("parseBitrate(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0", "-1K", "wat"} {
		if _, err := parseBitrate(in); err == nil {
			t.Fatalf("parseBitrate(%q) unexpectedly succeeded", in)
		}
	}
}

func TestLoadUsers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(path, []byte("username,password\nalice,secret\nbob,pass phrase\n"), 0600); err != nil {
		t.Fatal(err)
	}
	users, err := loadUsers(path)
	if err != nil {
		t.Fatal(err)
	}
	if users["alice"] != "secret" || users["bob"] != "pass phrase" {
		t.Fatalf("unexpected users: %#v", users)
	}
}

func TestCreateRecordingDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	first, err := createRecording(dir, "alice", ".h264")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := createRecording(dir, "alice", ".h264")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Name() == second.Name() {
		t.Fatal("recordings reused the same name")
	}
}

func TestCodecExtension(t *testing.T) {
	if ext, ok := codecExtension("h264", "annexb"); !ok || ext != ".h264" {
		t.Fatal("h264 Annex-B rejected")
	}
	if ext, ok := codecExtension("hevc", "annexb"); !ok || ext != ".h265" {
		t.Fatal("HEVC Annex-B rejected")
	}
	if _, ok := codecExtension("vp9", "annexb"); ok {
		t.Fatal("VP9 unexpectedly accepted")
	}
	if _, ok := codecExtension("h264", "avc"); ok {
		t.Fatal("non-Annex-B H.264 unexpectedly accepted")
	}
}

func TestRemoteIP(t *testing.T) {
	tests := map[string]string{
		"127.0.0.1:54321":   "127.0.0.1",
		"[2001:db8::1]:443": "2001:db8::1",
		"local-client":      "local-client",
	}
	for input, want := range tests {
		if got := remoteIP(input); got != want {
			t.Fatalf("remoteIP(%q) = %q; want %q", input, got, want)
		}
	}
}

func TestIsKeyframe(t *testing.T) {
	h264IDR := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x68, 0xce, 0, 0, 1, 0x65, 0x88}
	h264P := []byte{0, 0, 0, 1, 0x41, 0x9a}
	hevcIDR := []byte{0, 0, 0, 1, 0x40, 0x01, 0, 0, 0, 1, 0x26, 0x01, 0xaf}
	hevcP := []byte{0, 0, 0, 1, 0x02, 0x01, 0xd0}
	if !isKeyframe("h264", h264IDR) || isKeyframe("h264", h264P) {
		t.Fatal("h264 keyframe detection failed")
	}
	if !isKeyframe("hevc", hevcIDR) || isKeyframe("hevc", hevcP) {
		t.Fatal("hevc keyframe detection failed")
	}
}
