package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"container-tags/internal/refs"
)

func TestExtractVersionFromString(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"v1.2.3", "v1.2.3"},
		{"1.2.3", "1.2.3"},
		{"release v2.6.0 (stable)", "v2.6.0"},
		{"refs/tags/v3.4.5", "v3.4.5"},
		{"refs/tags/2.6.0", "2.6.0"},
		{"latest", ""},
		{"", ""},
		{"buildkit.dockerfile.v0", ""},
		{"nightly", ""},
	}
	for _, tc := range cases {
		got := extractVersionFromString(tc.in)
		if got != tc.want {
			t.Fatalf("extractVersionFromString(%q)=%q; want %q", tc.in, got, tc.want)
		}
	}
}

func TestPickVersionFromAnnotationsIgnoresTitleDescription(t *testing.T) {
	ann := map[string]string{
		"org.opencontainers.image.title":       "Apache NiFi",
		"org.opencontainers.image.description": "Based on OpenJDK v21.0.8 and Debian",
	}
	if v := pickVersionFromAnnotations(ann); v != "" {
		t.Fatalf("expected no version from title/description, got %q", v)
	}
	ann["org.opencontainers.image.version"] = "2.6.0"
	if v := pickVersionFromAnnotations(ann); v != "2.6.0" {
		t.Fatalf("expected version 2.6.0 from explicit annotation, got %q", v)
	}
}

func TestResolveTagAliasReusesDigestFromInference(t *testing.T) {
	var refHeadCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/latest":
			if r.Method == http.MethodGet {
				w.Header().Set("Docker-Content-Digest", "sha256:latest")
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				fmt.Fprint(w, `{"config":{"digest":"sha256:cfg-latest"}}`)
				return
			}
			if r.Method == http.MethodHead {
				refHeadCount.Add(1)
				w.Header().Set("Docker-Content-Digest", "sha256:latest")
				return
			}
		case "/v2/ns/app/manifests/v1.2.3":
			w.Header().Set("Docker-Content-Digest", "sha256:latest")
			return
		case "/v2/ns/app/manifests/v1.2.2":
			w.Header().Set("Docker-Content-Digest", "sha256:older")
			return
		case "/v2/ns/app/blobs/sha256:cfg-latest":
			fmt.Fprint(w, `{"config":{"labels":{}}}`)
			return
		case "/v2/ns/app/tags/list":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"ns/app","tags":["latest","v1.2.2","v1.2.3"]}`)
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	digest, tag, err := client.ResolveTagAlias(img, "latest")
	if err != nil {
		t.Fatalf("ResolveTagAlias returned error: %v", err)
	}
	if digest != "sha256:latest" {
		t.Fatalf("digest=%q, want sha256:latest", digest)
	}
	if tag != "v1.2.3" {
		t.Fatalf("tag=%q, want v1.2.3", tag)
	}
	if got := refHeadCount.Load(); got != 0 {
		t.Fatalf("expected no HEAD request for reference tag after inference, got %d", got)
	}
}

func TestResolveTagAliasChecksTagsConcurrently(t *testing.T) {
	const totalTags = 96
	const delayPerHead = 40 * time.Millisecond
	tags := make([]string, 0, totalTags+1)
	digests := make(map[string]string, totalTags+1)
	targetTag := "v1.2.95"
	targetDigest := "sha256:match"

	for i := 0; i < totalTags; i++ {
		tag := fmt.Sprintf("v1.2.%d", i)
		tags = append(tags, tag)
		digests[tag] = fmt.Sprintf("sha256:%d", i)
	}
	digests[targetTag] = targetDigest
	tags = append([]string{"latest"}, tags...)

	var active atomic.Int32
	var maxActive atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/latest":
			if r.Method == http.MethodGet {
				w.Header().Set("Docker-Content-Digest", targetDigest)
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				fmt.Fprint(w, `{"config":{"digest":"sha256:cfg-latest"}}`)
				return
			}
		case "/v2/ns/app/blobs/sha256:cfg-latest":
			fmt.Fprint(w, `{"config":{"labels":{}}}`)
			return
		case "/v2/ns/app/tags/list":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":"ns/app","tags":[%s]}`, quotedTags(tags))
			return
		}

		const prefix = "/v2/ns/app/manifests/"
		if r.Method == http.MethodHead && len(r.URL.Path) > len(prefix) && r.URL.Path[:len(prefix)] == prefix {
			tag := r.URL.Path[len(prefix):]
			if digest, ok := digests[tag]; ok {
				cur := active.Add(1)
				defer active.Add(-1)
				for {
					prev := maxActive.Load()
					if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
						break
					}
				}
				time.Sleep(delayPerHead)
				w.Header().Set("Docker-Content-Digest", digest)
				return
			}
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	start := time.Now()
	digest, tag, err := client.ResolveTagAlias(img, "latest")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ResolveTagAlias returned error: %v", err)
	}
	if digest != targetDigest {
		t.Fatalf("digest=%q, want %s", digest, targetDigest)
	}
	if tag != targetTag {
		t.Fatalf("tag=%q, want %s", tag, targetTag)
	}
	if got := maxActive.Load(); got < 2 {
		t.Fatalf("expected concurrent HEAD requests, max concurrency was %d", got)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("ResolveTagAlias took %v; want under 2s for concurrent scan", elapsed)
	}
}

func TestResolveTagAliasPrefersCommitLikeTagsForBranchRefs(t *testing.T) {
	tags := []string{"latest", "v1.2.3", "19c9e62", "7f2d4aa", "release-candidate"}
	digests := map[string]string{
		"v1.2.3":            "sha256:semver",
		"19c9e62":           "sha256:main",
		"7f2d4aa":           "sha256:other",
		"release-candidate": "sha256:misc",
	}
	var seenMu sync.Mutex
	var seen []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/main":
			if r.Method == http.MethodGet {
				w.Header().Set("Docker-Content-Digest", "sha256:main")
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				fmt.Fprint(w, `{"config":{"digest":"sha256:cfg-main"}}`)
				return
			}
		case "/v2/ns/app/blobs/sha256:cfg-main":
			fmt.Fprint(w, `{"config":{"labels":{}}}`)
			return
		case "/v2/ns/app/tags/list":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":"ns/app","tags":[%s]}`, quotedTags(tags))
			return
		}

		const prefix = "/v2/ns/app/manifests/"
		if r.Method == http.MethodHead && len(r.URL.Path) > len(prefix) && r.URL.Path[:len(prefix)] == prefix {
			tag := r.URL.Path[len(prefix):]
			if digest, ok := digests[tag]; ok {
				seenMu.Lock()
				seen = append(seen, tag)
				seenMu.Unlock()
				w.Header().Set("Docker-Content-Digest", digest)
				return
			}
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.cachePath = ""
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	_, tag, err := client.ResolveTagAlias(img, "main")
	if err != nil {
		t.Fatalf("ResolveTagAlias returned error: %v", err)
	}
	if tag != "19c9e62" {
		t.Fatalf("tag=%q, want 19c9e62", tag)
	}
	seenMu.Lock()
	defer seenMu.Unlock()
	ordered := prioritizeTags(tags, "main")
	if len(ordered) == 0 || ordered[0] != "19c9e62" {
		t.Fatalf("expected commit-like tag to be prioritized first, got %v", ordered)
	}
	if len(seen) == 0 {
		t.Fatalf("expected at least one probe, saw none")
	}
}

func TestManifestDigestForReferenceUsesMemoryCache(t *testing.T) {
	var headCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/ns/app/manifests/v1.2.3" && r.Method == http.MethodHead {
			headCount.Add(1)
			w.Header().Set("Docker-Content-Digest", "sha256:cached")
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.cachePath = ""

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		digest, err := client.manifestDigestForReference(ctx, server.URL, "ns/app", "v1.2.3")
		if err != nil {
			t.Fatalf("manifestDigestForReference returned error: %v", err)
		}
		if digest != "sha256:cached" {
			t.Fatalf("digest=%q, want sha256:cached", digest)
		}
	}
	if got := headCount.Load(); got != 1 {
		t.Fatalf("expected one HEAD request due to cache reuse, got %d", got)
	}
}

func TestClientLoadsPersistentCacheAcrossRuns(t *testing.T) {
	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "digest-cache.json")
	cache := diskCache{
		Version: 2,
		Digests: map[string]cacheEntry{
			digestCacheKey("https://example.invalid", "ns/app", "main"): {
				Digest:    "sha256:persisted",
				UpdatedAt: time.Now().UTC(),
			},
		},
		TagLists: map[string]tagListCacheEntry{
			tagListCacheKey("https://example.invalid", "ns/app"): {
				Tags:      []string{"latest", "main", "19c9e62"},
				UpdatedAt: time.Now().UTC(),
			},
		},
	}
	b, err := json.Marshal(cache)
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}
	if err := os.WriteFile(cachePath, b, 0o600); err != nil {
		t.Fatalf("os.WriteFile returned error: %v", err)
	}

	client := NewClient()
	client.cachePath = cachePath
	client.cache, client.tagLists = loadCaches(cachePath)

	digest, ok := client.cachedDigest("https://example.invalid", "ns/app", "main")
	if !ok {
		t.Fatal("expected persisted cache entry to be loaded")
	}
	if digest != "sha256:persisted" {
		t.Fatalf("digest=%q, want sha256:persisted", digest)
	}
	tags, ok := client.cachedTagList("https://example.invalid", "ns/app")
	if !ok {
		t.Fatal("expected persisted tag list to be loaded")
	}
	if !slicesEqual(tags, []string{"latest", "main", "19c9e62"}) {
		t.Fatalf("tags=%v, want persisted tag list", tags)
	}
}

func TestClientClosePersistsCache(t *testing.T) {
	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "digest-cache.json")
	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.cachePath = cachePath
	client.storeDigest("https://example.invalid", "ns/app", "v1.2.3", "sha256:saved")

	if err := client.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	b, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("os.ReadFile returned error: %v", err)
	}
	var cache diskCache
	if err := json.Unmarshal(b, &cache); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}
	entry, ok := cache.Digests[digestCacheKey("https://example.invalid", "ns/app", "v1.2.3")]
	if !ok {
		t.Fatal("expected persisted digest entry")
	}
	if entry.Digest != "sha256:saved" {
		t.Fatalf("digest=%q, want sha256:saved", entry.Digest)
	}
}

func TestTagsForRepositoryUsesMemoryCache(t *testing.T) {
	var tagListCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/ns/app/tags/list" {
			tagListCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"ns/app","tags":["latest","main","19c9e62"]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.tagLists = map[string]tagListCacheEntry{}
	client.cachePath = ""

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		tags, err := client.tagsForRepository(ctx, server.URL, "ns/app")
		if err != nil {
			t.Fatalf("tagsForRepository returned error: %v", err)
		}
		if !slicesEqual(tags, []string{"latest", "main", "19c9e62"}) {
			t.Fatalf("tags=%v, want expected tag list", tags)
		}
	}
	if got := tagListCount.Load(); got != 1 {
		t.Fatalf("expected one tags/list request due to cache reuse, got %d", got)
	}
}

func TestResolveTagAliasUsesCachedTagListOnSecondRun(t *testing.T) {
	var tagListCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/main":
			if r.Method == http.MethodHead {
				w.Header().Set("Docker-Content-Digest", "sha256:main")
				return
			}
		case "/v2/ns/app/manifests/19c9e62":
			w.Header().Set("Docker-Content-Digest", "sha256:main")
			return
		case "/v2/ns/app/tags/list":
			tagListCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"ns/app","tags":["latest","main","19c9e62"]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.tagLists = map[string]tagListCacheEntry{}
	client.cachePath = ""
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	for i := 0; i < 2; i++ {
		_, tag, err := client.ResolveTagAlias(img, "main")
		if err != nil {
			t.Fatalf("ResolveTagAlias returned error: %v", err)
		}
		if tag != "19c9e62" {
			t.Fatalf("tag=%q, want 19c9e62", tag)
		}
	}
	if got := tagListCount.Load(); got != 1 {
		t.Fatalf("expected one tags/list request across repeated runs, got %d", got)
	}
}

func TestResolveTagAliasSkipsInferenceForMutableRefs(t *testing.T) {
	var getMainCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/main":
			if r.Method == http.MethodGet {
				getMainCount.Add(1)
				w.Header().Set("Docker-Content-Digest", "sha256:main")
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				fmt.Fprint(w, `{"config":{"digest":"sha256:cfg-main"}}`)
				return
			}
			if r.Method == http.MethodHead {
				w.Header().Set("Docker-Content-Digest", "sha256:main")
				return
			}
		case "/v2/ns/app/manifests/19c9e62":
			w.Header().Set("Docker-Content-Digest", "sha256:main")
			return
		case "/v2/ns/app/tags/list":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"ns/app","tags":["latest","19c9e62"]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	client.cache = map[string]cacheEntry{}
	client.cachePath = ""
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	_, tag, err := client.ResolveTagAlias(img, "main")
	if err != nil {
		t.Fatalf("ResolveTagAlias returned error: %v", err)
	}
	if tag != "19c9e62" {
		t.Fatalf("tag=%q, want 19c9e62", tag)
	}
	if got := getMainCount.Load(); got != 0 {
		t.Fatalf("expected mutable ref to skip GET inference, got %d GET requests", got)
	}
}

func BenchmarkResolveTagAliasLargeTagSet(b *testing.B) {
	const totalTags = 240
	tags := make([]string, 0, totalTags+1)
	digests := make(map[string]string, totalTags+1)
	targetTag := "v1.2.239"
	targetDigest := "sha256:match"

	for i := 0; i < totalTags; i++ {
		tag := fmt.Sprintf("v1.2.%d", i)
		tags = append(tags, tag)
		digests[tag] = fmt.Sprintf("sha256:%d", i)
	}
	digests[targetTag] = targetDigest
	tags = append([]string{"latest"}, tags...)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/app/manifests/latest":
			if r.Method == http.MethodGet {
				w.Header().Set("Docker-Content-Digest", targetDigest)
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				fmt.Fprint(w, `{"config":{"digest":"sha256:cfg-latest"}}`)
				return
			}
		case "/v2/ns/app/blobs/sha256:cfg-latest":
			fmt.Fprint(w, `{"config":{"labels":{}}}`)
			return
		case "/v2/ns/app/tags/list":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":"ns/app","tags":[%s]}`, quotedTags(tags))
			return
		}

		const prefix = "/v2/ns/app/manifests/"
		if r.Method == http.MethodHead && len(r.URL.Path) > len(prefix) && r.URL.Path[:len(prefix)] == prefix {
			tag := r.URL.Path[len(prefix):]
			if digest, ok := digests[tag]; ok {
				w.Header().Set("Docker-Content-Digest", digest)
				return
			}
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient()
	img := refs.ImageRef{Registry: server.URL, Repository: "ns/app"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := client.ResolveTagAlias(img, "latest"); err != nil {
			b.Fatalf("ResolveTagAlias returned error: %v", err)
		}
	}
}

func quotedTags(tags []string) string {
	if len(tags) == 0 {
		return ""
	}

	parts := make([]string, len(tags))
	for i, tag := range tags {
		parts[i] = fmt.Sprintf("%q", tag)
	}
	return strings.Join(parts, ",")
}
