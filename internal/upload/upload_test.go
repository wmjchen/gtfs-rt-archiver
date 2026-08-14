package upload

import (
	"errors"
	"testing"
	"time"
)

func TestRemoteJoinAndErrorCategories(t *testing.T) {
	got := remoteJoin("remote:bucket/root/", "parquet/format=v1", "manifest.json")
	if got != "remote:bucket/root/parquet/format=v1/manifest.json" {
		t.Fatalf("remote path = %q", got)
	}
	if got := classifyError(errors.New("exit status 1"), "Access Denied"); got != "permission" {
		t.Fatalf("category = %q", got)
	}
	if !isPermanent("remote_integrity") || isPermanent("network") || isPermanent("authentication") {
		t.Fatal("permanence classification is wrong")
	}
}

func TestBackoffIsBounded(t *testing.T) {
	for attempt := 1; attempt < 20; attempt++ {
		if got := backoff(attempt); got < 0 || got > 6*time.Hour {
			t.Fatalf("attempt %d backoff %s", attempt, got)
		}
	}
}
