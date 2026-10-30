package xiaomi

import (
	"maps"
	"net/url"
	"strings"
	"testing"
)

const reusePrimary = "xiaomi://user:cn@192.0.2.1?did=123&model=xiaomi.camera.082ac1&subtype=3"
const reuseSecondary = reusePrimary + "&channel=2&reuse=primary"

func TestReuseScope(t *testing.T) {
	mono := strings.Replace(reusePrimary, "082ac1", "81ac1", 1)
	sources := map[string][]string{
		"primary": {reusePrimary}, "secondary": {reuseSecondary}, "mono": {mono},
		"unrelated": {"xiaomi://other:cn@192.0.2.2?channel=2&reuse=missing"},
	}
	for _, source := range []string{reusePrimary, reuseSecondary, mono} {
		plan, err := planReuse(reuseScope(source, sources))
		if err != nil {
			t.Fatal("an unrelated invalid declaration disabled this camera", err)
		}
		if source == mono && len(plan) != 0 || source != mono && len(plan) != 2 {
			t.Fatal("scope lost the explicit device dependency")
		}
	}
}

func TestReuse(t *testing.T) {
	sources := map[string][]string{
		"secondary": {reuseSecondary},
		"primary":   {"ffmpeg:primary#audio=opus/24000", reusePrimary},
	}
	plan, err := planReuse(sources)
	if err != nil {
		t.Fatal(err)
	}
	primary, _ := url.Parse(plan[reusePrimary].url)
	if primary.Query().Get("channel") != "all" || sources["primary"][1] != reusePrimary || len(plan) != 2 {
		t.Fatal("plan changed the configuration or lost a source")
	}
	if plan[reuseSecondary].stream != "primary" || plan[reuseSecondary].url != "" {
		t.Fatal("lost explicit stream ownership")
	}
	for _, source := range []string{reusePrimary, reuseSecondary} {
		if got, err := resolveReuse(source, plan, sources); err != nil || got != plan[source] {
			t.Fatal(got, err)
		}
	}
	// Declared primary options can be inherited by omitting them on the second URL.
	sources["secondary"] = []string{strings.Replace(reuseSecondary, "&subtype=3", "", 1)}
	if _, err = planReuse(sources); err != nil {
		t.Fatal(err)
	}
	sources = map[string][]string{"primary": {reusePrimary}}
	plan, err = planReuse(sources)
	got, resolveErr := resolveReuse(reusePrimary, plan, sources)
	if err != nil || resolveErr != nil || len(plan) != 0 || got.url != reusePrimary || got.stream != "" {
		t.Fatal("ordinary single-channel source changed", err, resolveErr)
	}
}

func TestReuseRejectsInvalidDependencies(t *testing.T) {
	for _, tc := range []struct {
		name, primary, secondary string
	}{
		{"unknown", "", reuseSecondary},
		{"self", reusePrimary, strings.Replace(reuseSecondary, "reuse=primary", "reuse=secondary", 1)},
		{"cycle", reusePrimary + "&channel=2&reuse=secondary", reuseSecondary},
		{"channel", reusePrimary, strings.Replace(reuseSecondary, "channel=2", "channel=1", 1)},
		{"empty", reusePrimary, strings.TrimSuffix(reuseSecondary, "primary")},
		{"duplicate parameter", reusePrimary, reuseSecondary + "&reuse=primary"},
		{"account", reusePrimary, strings.Replace(reuseSecondary, "user:cn", "other:cn", 1)},
		{"region", reusePrimary, strings.Replace(reuseSecondary, "user:cn", "user:de", 1)},
		{"host", reusePrimary, strings.Replace(reuseSecondary, "192.0.2.1", "192.0.2.2", 1)},
		{"device", reusePrimary, strings.Replace(reuseSecondary, "did=123", "did=456", 1)},
		{"model", reusePrimary, strings.Replace(reuseSecondary, "082ac1", "81ac1", 1)},
		{"quality", reusePrimary, strings.Replace(reuseSecondary, "subtype=3", "subtype=1", 1)},
		{"ptime", reusePrimary, reuseSecondary + "&ptime=20"},
		{"audio", reusePrimary + "&audio=0", reuseSecondary + "&audio=1"},
		{"secondary owner", reusePrimary + "&channel=2", reuseSecondary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]string{"primary": {tc.primary}, "secondary": {tc.secondary}}
			if _, err := planReuse(sources); err == nil {
				t.Fatal("invalid dependency accepted")
			}
		})
	}
	for _, duplicate := range []map[string][]string{
		{"primary": {reusePrimary, reusePrimary}, "secondary": {reuseSecondary}},
		{"primary": {reusePrimary}, "secondary": {reuseSecondary}, "other": {reusePrimary}},
	} {
		if _, err := planReuse(duplicate); err == nil {
			t.Fatal("ambiguous device ownership accepted")
		}
	}
}

func TestReuseAPIChangesRequireRestart(t *testing.T) {
	sources := map[string][]string{"primary": {reusePrimary}, "secondary": {reuseSecondary}}
	planned, err := planReuse(sources)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []map[string][]string{
		{"primary": {reusePrimary}}, // Remove second declaration.
		{"primary": {reusePrimary + "&audio=0"}, "secondary": {reuseSecondary}},
		{"primary": {reusePrimary}, "secondary": {reuseSecondary}, "third": {reusePrimary + "&channel=2&reuse=primary&audio=1"}},
	} {
		if _, err = resolveReuse(reuseSecondary, planned, change); err == nil {
			t.Fatal("stale ownership plan accepted")
		}
	}
	if _, err = resolveReuse(reuseSecondary, nil, sources); err == nil {
		t.Fatal("running single-channel producer could be silently upgraded")
	}
	if _, err = resolveReuse(reuseSecondary, maps.Clone(planned), map[string][]string{}); err == nil {
		t.Fatal("orphan projection accepted")
	}
}
