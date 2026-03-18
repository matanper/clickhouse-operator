package controller

import "testing"

func TestBuildVersionProbeJobNameTruncatesLongPrefix(t *testing.T) {
	t.Parallel()

	prefix := "very-long-clickhouse-cluster-name-for-test-case-123456"
	hash := "c0ed189d"

	got := buildVersionProbeJobName(prefix, hash)
	maxPrefixLen := maxLabelValueLength - len(versionProbeNameSuffix) - len(hash)
	want := prefix[:maxPrefixLen] + versionProbeNameSuffix + hash

	if got != want {
		t.Fatalf("unexpected job name:\nwant: %q\ngot:  %q", want, got)
	}
	if len(got) > maxLabelValueLength {
		t.Fatalf("job name exceeds max label length: %d", len(got))
	}
}

func TestBuildVersionProbeJobNameKeepsShortPrefix(t *testing.T) {
	t.Parallel()

	prefix := "short-clickhouse"
	hash := "deadbeef"

	got := buildVersionProbeJobName(prefix, hash)
	want := "short-clickhouse-version-probe-deadbeef"

	if got != want {
		t.Fatalf("unexpected job name:\nwant: %q\ngot:  %q", want, got)
	}
}
