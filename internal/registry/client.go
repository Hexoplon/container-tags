package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"container-tags/internal/refs"
	"os"
)

var (
	// ErrNoMatchingTag is returned when no tag matches the target digest.
	ErrNoMatchingTag = errors.New("no matching tag")
	semverTagRe      = regexp.MustCompile(`^(?:v)?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z\.-]+)?$`)
	shaLikeTagRe     = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
)

const manifestCheckConcurrency = 24

type cacheEntry struct {
	Digest    string    `json:"digest"`
	UpdatedAt time.Time `json:"updated_at"`
}

type tagListCacheEntry struct {
	Tags      []string  `json:"tags"`
	UpdatedAt time.Time `json:"updated_at"`
}

type diskCache struct {
	Version  int                          `json:"version"`
	Digests  map[string]cacheEntry        `json:"digests,omitempty"`
	TagLists map[string]tagListCacheEntry `json:"tag_lists,omitempty"`
	Entries  map[string]cacheEntry        `json:"entries,omitempty"`
}

// Client talks to a Docker Registry v2 API compatible registry.
type Client struct {
	http *http.Client
	// repo -> bearer token
	tokens     map[string]string
	tokensMu   sync.RWMutex
	authMu     sync.Mutex
	cache      map[string]cacheEntry
	tagLists   map[string]tagListCacheEntry
	cacheMu    sync.RWMutex
	cachePath  string
	cacheDirty bool
	// slog logger (defaults to WARN level to stay quiet)
	logger *slog.Logger
	// Optional credentials for token exchange
	username     string
	password     string
	staticBearer string
}

func NewClient() *Client {
	cachePath := defaultCachePath()
	digestCache, tagListCache := loadCaches(cachePath)
	return &Client{
		http:      &http.Client{Timeout: 20 * time.Second},
		tokens:    make(map[string]string),
		cache:     digestCache,
		tagLists:  tagListCache,
		cachePath: cachePath,
		logger:    slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

// SetVerbosity controls logging level: 0 quiet (warn), 1 info, 2 debug.
func (c *Client) SetVerbosity(v int) {
	lvl := slog.LevelWarn
	if v >= 2 {
		lvl = slog.LevelDebug
	} else if v == 1 {
		lvl = slog.LevelInfo
	}
	c.logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

// SetLogger allows injecting a custom slog.Logger.
func (c *Client) SetLogger(l *slog.Logger) { c.logger = l }

// SetBasicAuth sets optional basic credentials used when exchanging tokens.
func (c *Client) SetBasicAuth(user, pass string) {
	c.username = user
	c.password = pass
}

// SetStaticBearer sets an explicit registry token to use directly.
func (c *Client) SetStaticBearer(tok string) { c.staticBearer = tok }

func (c *Client) Close() error {
	return c.saveCache()
}

// ResolveTagAlias resolves which concrete tag (e.g., v1.2.3) has the same
// manifest digest as the provided reference tag (default "latest").
// It returns the digest for the reference tag and the matched concrete tag.
func (c *Client) ResolveTagAlias(img refs.ImageRef, referenceTag string) (string, string, error) {
	base := registryBase(img.Registry)
	if base == "" {
		return "", "", fmt.Errorf("unsupported registry: %s", img.Registry)
	}

	ctx := context.Background()
	repo := img.Repository

	c.logInfo("registry=%s repo=%s refTag=%s base=%s", img.Registry, repo, referenceTag, base)

	var digest string
	inferFirst := shouldInferBeforeScan(referenceTag)

	// Fast path: infer version from labels on the reference tag's manifest/config.
	if inferFirst {
		if d, v, ok, err := c.InferVersionFromTag(img, referenceTag); err == nil && ok {
			c.logInfo("inferred version %q from labels (digest %s)", v, d)
			return d, v, nil
		} else if d != "" {
			digest = d
		} else if err != nil {
			c.logDebug("infer version failed: %v", err)
		} else {
			c.logDebug("infer version: no version labels found")
		}
	} else {
		c.logDebug("skipping label inference for mutable ref %s", referenceTag)
	}

	// Reuse the digest already observed while inspecting the reference tag.
	if digest == "" {
		c.logInfo("fetching digest for %s:%s", repo, referenceTag)
		var err error
		digest, err = c.manifestDigestForReference(ctx, base, repo, referenceTag)
		if err != nil {
			return "", "", err
		}
	} else {
		c.logDebug("reusing digest from manifest inspection for %s:%s", repo, referenceTag)
	}
	c.logInfo("digest for %s:%s is %s", repo, referenceTag, digest)

	tags, err := c.tagsForRepository(ctx, base, repo)
	if err != nil {
		return digest, "", err
	}
	c.logInfo("found %d tags", len(tags))

	orderedTags := prioritizeTags(tags, referenceTag)

	if match, err := c.findMatchingTag(ctx, base, repo, orderedTags, digest); err != nil {
		return digest, "", err
	} else if match != "" {
		return digest, match, nil
	}

	return digest, "", ErrNoMatchingTag
}

func (c *Client) findMatchingTag(ctx context.Context, base, repo string, tags []string, targetDigest string) (string, error) {
	if len(tags) == 0 {
		return "", nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type tagJob struct {
		index int
		tag   string
	}
	type tagResult struct {
		index  int
		digest string
	}

	workerCount := manifestCheckConcurrency
	if workerCount > len(tags) {
		workerCount = len(tags)
	}

	jobs := make(chan tagJob, workerCount)
	results := make(chan tagResult, workerCount)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					c.logDebug("checking tag %s", job.tag)
					d, err := c.manifestDigestForReference(ctx, base, repo, job.tag)
					if err != nil {
						c.logDebug("error checking %s: %v", job.tag, err)
					}
					select {
					case <-ctx.Done():
						return
					case results <- tagResult{index: job.index, digest: d}:
					}
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for i, tag := range tags {
			select {
			case <-ctx.Done():
				return
			case jobs <- tagJob{index: i, tag: tag}:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	resolved := make(map[int]string, len(tags))
	nextIndex := 0
	for res := range results {
		resolved[res.index] = res.digest
		for {
			digest, ok := resolved[nextIndex]
			if !ok {
				break
			}
			delete(resolved, nextIndex)
			if digest == targetDigest {
				match := tags[nextIndex]
				c.logInfo("match: %s (digest %s)", match, targetDigest)
				cancel()
				return match, nil
			}
			nextIndex++
			if nextIndex == len(tags) {
				return "", nil
			}
		}
	}

	return "", nil
}

// InferVersionFromTag attempts to read a version string for the given tag by
// inspecting manifest annotations and the image config labels. It returns the
// manifest digest, the inferred version, and whether a version was found.
func (c *Client) InferVersionFromTag(img refs.ImageRef, tag string) (digest string, version string, found bool, err error) {
	base := registryBase(img.Registry)
	repo := img.Repository
	ctx := context.Background()

	// GET manifest for the tag to obtain digest, content-type, and payload
	mURL := fmt.Sprintf("%s/v2/%s/manifests/%s", base, repo, url.PathEscape(tag))
	c.logInfo("GET %s", mURL)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, mURL, nil)
	req.Header.Set("Accept", strings.Join([]string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	}, ", "))
	resp, err := c.doWithAuth(ctx, base, repo, req)
	if err != nil {
		return "", "", false, err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", false, fmt.Errorf("manifest GET %s: %s", tag, resp.Status)
	}
	digest = resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		c.logDebug("no Docker-Content-Digest header on GET; will still parse payload")
	}
	ct := resp.Header.Get("Content-Type")
	c.logDebug("manifest content-type=%s digest=%s", ct, digest)

	// Try manifest index first
	if strings.Contains(ct, "application/vnd.oci.image.index.v1+json") || strings.Contains(ct, "application/vnd.docker.distribution.manifest.list.v2+json") {
		// Minimal index type
		var idx struct {
			Annotations map[string]string `json:"annotations"`
			Manifests   []struct {
				Digest      string            `json:"digest"`
				MediaType   string            `json:"mediaType"`
				Annotations map[string]string `json:"annotations"`
				Platform    struct {
					OS           string `json:"os"`
					Architecture string `json:"architecture"`
				} `json:"platform"`
			} `json:"manifests"`
		}
		if err := json.Unmarshal(body, &idx); err != nil {
			return digest, "", false, fmt.Errorf("parse index: %w", err)
		}
		if v := pickVersionFromAnnotations(idx.Annotations); v != "" {
			return digest, v, true, nil
		}
		// Check per-manifest annotations for a version before fetching configs
		for _, m := range idx.Manifests {
			if v := pickVersionFromAnnotations(m.Annotations); v != "" {
				return digest, v, true, nil
			}
		}
		// Choose linux/amd64 if present, else first
		chosen := ""
		for _, m := range idx.Manifests {
			if strings.EqualFold(m.Platform.OS, "linux") && strings.EqualFold(m.Platform.Architecture, "amd64") {
				chosen = m.Digest
				break
			}
		}
		if chosen == "" && len(idx.Manifests) > 0 {
			chosen = idx.Manifests[0].Digest
		}
		if chosen == "" {
			return digest, "", false, nil
		}
		// Try preferred manifest first; if not found, try the rest.
		d2, v2, ok2, err2 := c.inferVersionFromManifestDigest(ctx, base, repo, chosen, digest)
		if err2 != nil {
			return d2, v2, ok2, err2
		}
		if ok2 {
			return d2, v2, true, nil
		}
		for _, m := range idx.Manifests {
			if m.Digest == chosen {
				continue
			}
			d3, v3, ok3, err3 := c.inferVersionFromManifestDigest(ctx, base, repo, m.Digest, digest)
			if err3 != nil {
				continue
			}
			if ok3 {
				return d3, v3, true, nil
			}
		}
		return digest, "", false, nil
	}

	// Otherwise, treat as single manifest
	return c.inferVersionFromManifestPayload(ctx, base, repo, body, digest)
}

func (c *Client) inferVersionFromManifestDigest(ctx context.Context, base, repo, manifestDigest, outerDigest string) (string, string, bool, error) {
	u := fmt.Sprintf("%s/v2/%s/manifests/%s", base, repo, url.PathEscape(manifestDigest))
	c.logInfo("GET %s", u)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Accept", strings.Join([]string{
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	}, ", "))
	resp, err := c.doWithAuth(ctx, base, repo, req)
	if err != nil {
		return outerDigest, "", false, err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return outerDigest, "", false, fmt.Errorf("manifest GET by digest: %s", resp.Status)
	}
	// Prefer this digest if header present
	if d := resp.Header.Get("Docker-Content-Digest"); d != "" {
		outerDigest = d
	}
	return c.inferVersionFromManifestPayload(ctx, base, repo, body, outerDigest)
}

func (c *Client) inferVersionFromManifestPayload(ctx context.Context, base, repo string, body []byte, digest string) (string, string, bool, error) {
	// Minimal manifest type
	var man struct {
		Annotations map[string]string `json:"annotations"`
		Config      struct {
			Digest string `json:"digest"`
		} `json:"config"`
	}
	if err := json.Unmarshal(body, &man); err != nil {
		return digest, "", false, fmt.Errorf("parse manifest: %w", err)
	}
	if v := pickVersionFromAnnotations(man.Annotations); v != "" {
		return digest, v, true, nil
	}
	if man.Config.Digest == "" {
		return digest, "", false, nil
	}
	// Fetch image config blob and inspect labels
	bu := fmt.Sprintf("%s/v2/%s/blobs/%s", base, repo, url.PathEscape(man.Config.Digest))
	c.logInfo("GET %s", bu)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, bu, nil)
	resp, err := c.doWithAuth(ctx, base, repo, req)
	if err != nil {
		return digest, "", false, err
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return digest, "", false, fmt.Errorf("config blob GET: %s", resp.Status)
	}
	// Docker/OCI image config structure has config.Labels
	// Be liberal in what we accept: Docker uses `Labels` and OCI uses `labels`.
	var cfg struct {
		Config struct {
			LabelsUpper map[string]string `json:"Labels"`
			LabelsLower map[string]string `json:"labels"`
			Env         []string          `json:"Env"`
		} `json:"config"`
		ConfigUpper struct {
			LabelsUpper map[string]string `json:"Labels"`
			LabelsLower map[string]string `json:"labels"`
		} `json:"Config"`
		LabelsUpper     map[string]string `json:"Labels"`
		LabelsLower     map[string]string `json:"labels"`
		ContainerConfig struct {
			LabelsUpper map[string]string `json:"Labels"`
			LabelsLower map[string]string `json:"labels"`
			Env         []string          `json:"Env"`
		} `json:"container_config"`
		History []struct {
			CreatedBy string `json:"created_by"`
			Comment   string `json:"comment"`
		} `json:"history"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return digest, "", false, fmt.Errorf("parse config: %w", err)
	}
	// Merge possible label locations
	labels := map[string]string{}
	for k, v := range cfg.Config.LabelsUpper {
		labels[k] = v
	}
	for k, v := range cfg.Config.LabelsLower {
		labels[k] = v
	}
	for k, v := range cfg.ConfigUpper.LabelsUpper {
		labels[k] = v
	}
	for k, v := range cfg.ConfigUpper.LabelsLower {
		labels[k] = v
	}
	for k, v := range cfg.LabelsUpper {
		labels[k] = v
	}
	for k, v := range cfg.LabelsLower {
		labels[k] = v
	}
	for k, v := range cfg.ContainerConfig.LabelsUpper {
		labels[k] = v
	}
	for k, v := range cfg.ContainerConfig.LabelsLower {
		labels[k] = v
	}
	if len(labels) == 0 {
		c.logDebug("no labels found in config blob")
		return digest, "", false, nil
	}
	// Look for common version labels (and try to extract semver-looking values).
	// Be conservative to avoid picking base image/runtime versions (e.g., JAVA_VERSION).
	keys := []string{
		"alpha.talos.dev/version",
		"org.opencontainers.image.version",
		"org.label-schema.version",
		"io.artifacthub.package.version",
		"app.kubernetes.io/version",
		"build.version",
		"vcs.tag",
		"org.opencontainers.image.ref.name",
		// Intentionally exclude title/description: they often contain unrelated versions
		// like OS, JVM or tooling versions.
		"version",
	}
	for _, k := range keys {
		if v, ok := labels[k]; ok {
			if vv := extractVersionFromString(v); vv != "" {
				c.logInfo("inferred from label key=%s value=%s", k, vv)
				return digest, vv, true, nil
			}
		}
	}

	// Avoid using generic ENV/history heuristics as they often include
	// base image or runtime versions (e.g., JAVA_VERSION=21.0.8) which are
	// unrelated to the application version.
	return digest, "", false, nil
}

func pickVersionFromAnnotations(ann map[string]string) string {
	if ann == nil {
		return ""
	}
	// Try a set of likely annotation keys and extract semver-looking parts if needed
	for _, k := range []string{
		"org.opencontainers.image.version",
		"org.label-schema.version",
		"io.artifacthub.package.version",
		"app.kubernetes.io/version",
		"vcs.tag",
		"org.opencontainers.image.ref.name",
		"version",
	} {
		if vv := extractVersionFromString(ann[k]); vv != "" {
			return vv
		}
	}
	return ""
}

// extractVersionFromString attempts to normalize a label or annotation value into
// a tag-like version string. It accepts simple values (e.g., "v1.2.3") or
// extracts the first semver-looking token from longer strings (e.g., "talosctl v1.6.3").
func extractVersionFromString(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "latest") {
		return ""
	}
	// Direct match is good enough
	semverRe := regexp.MustCompile(`(?i)\b(?:v)?\d+\.\d+\.\d+(?:[.-][0-9A-Za-z\.-]+)?\b`)
	if semverRe.MatchString(s) {
		m := semverRe.FindString(s)
		// preserve original case of 'v' if present
		return strings.TrimSpace(m)
	}
	// Some projects use refs/tags/<ver>
	if strings.Contains(s, "refs/tags/") {
		part := s[strings.LastIndex(s, "/")+1:]
		if semverRe.MatchString(part) {
			return semverRe.FindString(part)
		}
	}
	return ""
}

func registryBase(reg string) string {
	switch reg {
	case "docker.io":
		return "https://registry-1.docker.io"
	case "ghcr.io":
		return "https://ghcr.io"
	default:
		// Fallback: assume registry base is the hostname itself over HTTPS
		if strings.Contains(reg, "://") {
			return reg
		}
		return "https://" + reg
	}
}

func (c *Client) listAllTags(ctx context.Context, base, repo string) ([]string, error) {
	var tags []string
	// Use pagination where supported. Start with a decent page size.
	nextURL := fmt.Sprintf("%s/v2/%s/tags/list?n=1000", base, repo)
	for {
		c.logInfo("GET %s", nextURL)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		req.Header.Set("Accept", "application/json")
		resp, err := c.doWithAuth(ctx, base, repo, req)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("list tags failed: %s: %s", resp.Status, string(body))
		}
		var out struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, err
		}
		tags = append(tags, out.Tags...)
		c.logInfo("page returned %d tags (total %d)", len(out.Tags), len(tags))

		// Parse Link: <url>; rel="next"
		link := resp.Header.Get("Link")
		if link == "" {
			break
		}
		// Very small parser for RFC5988-style header
		// Example: <https://registry-1.docker.io/v2/library/nginx/tags/list?next_page=...>; rel="next"
		var next string
		for _, part := range strings.Split(link, ",") {
			p := strings.TrimSpace(part)
			if strings.HasSuffix(p, "rel=\"next\"") {
				if i := strings.Index(p, "<"); i != -1 {
					if j := strings.Index(p, ">"); j != -1 && j > i+1 {
						next = p[i+1 : j]
						break
					}
				}
			}
		}
		if next == "" {
			break
		}
		// Normalize next to absolute URL using base if it's relative
		if u, err := url.Parse(next); err == nil {
			if !u.IsAbs() {
				if bu, err2 := url.Parse(base); err2 == nil {
					nextURL = bu.ResolveReference(u).String()
				} else {
					nextURL = base + strings.TrimPrefix(next, "/")
				}
			} else {
				nextURL = u.String()
			}
		} else {
			// Fallback: best-effort join
			if strings.HasPrefix(next, "/") {
				nextURL = base + next
			} else {
				nextURL = base + "/" + next
			}
		}
		c.logDebug("pagination next resolved=%s", nextURL)
	}
	return tags, nil
}

func (c *Client) tagsForRepository(ctx context.Context, base, repo string) ([]string, error) {
	if tags, ok := c.cachedTagList(base, repo); ok {
		c.logDebug("tag list cache hit for %s", repo)
		return tags, nil
	}

	tags, err := c.listAllTags(ctx, base, repo)
	if err != nil {
		return nil, err
	}
	c.storeTagList(base, repo, tags)
	return tags, nil
}

func (c *Client) headManifestDigest(ctx context.Context, base, repo, reference string) (string, error) {
	url := fmt.Sprintf("%s/v2/%s/manifests/%s", base, repo, url.PathEscape(reference))
	c.logDebug("HEAD %s", url)
	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	// Ask for both OCI and Docker schema2 manifests and indexes
	req.Header.Set("Accept", strings.Join([]string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	}, ", "))

	resp, err := c.doWithAuth(ctx, base, repo, req)
	if err != nil {
		return "", err
	}
	// Some registries may not allow HEAD; fallback to GET to extract headers.
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		c.logDebug("fallback to GET for %s", url)
		req.Method = http.MethodGet
		resp, err = c.doWithAuth(ctx, base, repo, req)
		if err != nil {
			return "", err
		}
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK { // GET returns 200; HEAD often returns 200 as well
		return "", fmt.Errorf("manifest %s: %s", reference, resp.Status)
	}

	digest := resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		return "", fmt.Errorf("manifest %s: missing digest header", reference)
	}
	return digest, nil
}

func (c *Client) manifestDigestForReference(ctx context.Context, base, repo, reference string) (string, error) {
	if digest, ok := c.cachedDigest(base, repo, reference); ok {
		c.logDebug("cache hit for %s:%s", repo, reference)
		return digest, nil
	}

	digest, err := c.headManifestDigest(ctx, base, repo, reference)
	if err != nil {
		return "", err
	}
	c.storeDigest(base, repo, reference, digest)
	return digest, nil
}

func (c *Client) doWithAuth(ctx context.Context, base, repo string, req *http.Request) (*http.Response, error) {
	usedToken := ""
	if tok := c.getToken(repo); tok != "" {
		usedToken = tok
		req.Header.Set("Authorization", "Bearer "+tok)
	} else if c.staticBearer != "" {
		usedToken = c.staticBearer
		req.Header.Set("Authorization", "Bearer "+c.staticBearer)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	c.authMu.Lock()
	defer c.authMu.Unlock()

	if tok := c.getToken(repo); tok != "" && tok != usedToken {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		req2 := req.Clone(ctx)
		req2.Header.Set("Authorization", "Bearer "+tok)
		return c.http.Do(req2)
	}

	// Parse WWW-Authenticate to obtain a token
	www := resp.Header.Get("Www-Authenticate")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if www == "" {
		return nil, fmt.Errorf("unauthorized and no authenticate challenge")
	}
	realm, service, scope, err := parseAuthChallenge(www, repo)
	if err != nil {
		return nil, err
	}
	c.logInfo("authenticating realm=%s service=%s scope=%s", realm, service, scope)
	tok, err := fetchToken(ctx, realm, service, scope, c.username, c.password)
	if err != nil {
		return nil, err
	}
	c.setToken(repo, tok)

	req2 := req.Clone(ctx)
	req2.Header.Set("Authorization", "Bearer "+tok)
	return c.http.Do(req2)
}

func (c *Client) getToken(repo string) string {
	c.tokensMu.RLock()
	defer c.tokensMu.RUnlock()
	return c.tokens[repo]
}

func (c *Client) setToken(repo, token string) {
	c.tokensMu.Lock()
	defer c.tokensMu.Unlock()
	c.tokens[repo] = token
}

func (c *Client) cachedDigest(base, repo, reference string) (string, bool) {
	key := digestCacheKey(base, repo, reference)
	c.cacheMu.RLock()
	entry, ok := c.cache[key]
	c.cacheMu.RUnlock()
	if !ok {
		return "", false
	}
	if time.Since(entry.UpdatedAt) > digestCacheTTL(reference) {
		return "", false
	}
	return entry.Digest, true
}

func (c *Client) storeDigest(base, repo, reference, digest string) {
	if digest == "" {
		return
	}
	key := digestCacheKey(base, repo, reference)
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cache == nil {
		c.cache = make(map[string]cacheEntry)
	}
	entry := cacheEntry{Digest: digest, UpdatedAt: time.Now().UTC()}
	if existing, ok := c.cache[key]; ok && existing.Digest == entry.Digest && existing.UpdatedAt.After(entry.UpdatedAt.Add(-time.Second)) {
		return
	}
	c.cache[key] = entry
	c.cacheDirty = true
}

func (c *Client) cachedTagList(base, repo string) ([]string, bool) {
	key := tagListCacheKey(base, repo)
	c.cacheMu.RLock()
	entry, ok := c.tagLists[key]
	c.cacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Since(entry.UpdatedAt) > tagListCacheTTL(repo) {
		return nil, false
	}
	return append([]string(nil), entry.Tags...), true
}

func (c *Client) storeTagList(base, repo string, tags []string) {
	key := tagListCacheKey(base, repo)
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.tagLists == nil {
		c.tagLists = make(map[string]tagListCacheEntry)
	}
	entry := tagListCacheEntry{Tags: append([]string(nil), tags...), UpdatedAt: time.Now().UTC()}
	if existing, ok := c.tagLists[key]; ok && slicesEqual(existing.Tags, entry.Tags) && existing.UpdatedAt.After(entry.UpdatedAt.Add(-time.Second)) {
		return
	}
	c.tagLists[key] = entry
	c.cacheDirty = true
}

func (c *Client) saveCache() error {
	path, digests, tagLists, ok := c.cacheSnapshot()
	if !ok {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(diskCache{Version: 2, Digests: digests, TagLists: tagLists}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		return err
	}
	c.cacheMu.Lock()
	c.cacheDirty = false
	c.cacheMu.Unlock()
	return nil
}

func (c *Client) cacheSnapshot() (string, map[string]cacheEntry, map[string]tagListCacheEntry, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	if !c.cacheDirty || c.cachePath == "" {
		return "", nil, nil, false
	}

	digests := make(map[string]cacheEntry, len(c.cache))
	for k, v := range c.cache {
		digests[k] = v
	}

	tagLists := make(map[string]tagListCacheEntry, len(c.tagLists))
	for k, v := range c.tagLists {
		tagLists[k] = tagListCacheEntry{Tags: append([]string(nil), v.Tags...), UpdatedAt: v.UpdatedAt}
	}

	return c.cachePath, digests, tagLists, true
}

func loadCaches(path string) (map[string]cacheEntry, map[string]tagListCacheEntry) {
	if path == "" {
		return make(map[string]cacheEntry), make(map[string]tagListCacheEntry)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]cacheEntry), make(map[string]tagListCacheEntry)
	}
	var cache diskCache
	if err := json.Unmarshal(b, &cache); err != nil {
		return make(map[string]cacheEntry), make(map[string]tagListCacheEntry)
	}
	digests := cache.Digests
	if digests == nil {
		digests = cache.Entries
	}
	if digests == nil {
		digests = make(map[string]cacheEntry)
	}
	tagLists := cache.TagLists
	if tagLists == nil {
		tagLists = make(map[string]tagListCacheEntry)
	}
	return digests, tagLists
}

func defaultCachePath() string {
	if p := os.Getenv("CONTAINER_TAGS_CACHE_PATH"); p != "" {
		return p
	}
	if dir, err := os.UserCacheDir(); err == nil && dir != "" {
		return filepath.Join(dir, "container-tags", "digest-cache.json")
	}
	return ""
}

func digestCacheKey(base, repo, reference string) string {
	return base + "|" + repo + "|" + reference
}

func tagListCacheKey(base, repo string) string {
	return base + "|" + repo
}

func digestCacheTTL(reference string) time.Duration {
	switch {
	case strings.EqualFold(reference, "latest"):
		return 5 * time.Minute
	case semverTagRe.MatchString(reference), shaLikeTagRe.MatchString(reference):
		return 7 * 24 * time.Hour
	case looksMutableTag(reference):
		return 5 * time.Minute
	default:
		return time.Hour
	}
}

func tagListCacheTTL(repo string) time.Duration {
	if strings.Contains(strings.ToLower(repo), "nightly") {
		return 2 * time.Minute
	}
	return 5 * time.Minute
}

func looksMutableTag(tag string) bool {
	low := strings.ToLower(tag)
	for _, prefix := range []string{"main", "master", "develop", "dev", "edge", "nightly", "stable", "canary"} {
		if low == prefix || strings.HasPrefix(low, prefix+"-") || strings.HasSuffix(low, "-"+prefix) {
			return true
		}
	}
	return strings.Contains(low, "snapshot") || strings.Contains(low, "branch")
}

func shouldInferBeforeScan(referenceTag string) bool {
	if referenceTag == "" || strings.EqualFold(referenceTag, "latest") {
		return true
	}
	if semverTagRe.MatchString(referenceTag) {
		return true
	}
	if shaLikeTagRe.MatchString(referenceTag) || looksMutableTag(referenceTag) {
		return false
	}
	return true
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func prioritizeTags(tags []string, referenceTag string) []string {
	ordered := make([]string, 0, len(tags))
	for _, tag := range tags {
		if tag == referenceTag || strings.EqualFold(tag, "latest") {
			continue
		}
		ordered = append(ordered, tag)
	}

	ref := strings.ToLower(referenceTag)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := tagPriority(ordered[i], ref)
		right := tagPriority(ordered[j], ref)
		if left != right {
			return left < right
		}
		return ordered[i] < ordered[j]
	})
	return ordered
}

func tagPriority(tag, lowerRef string) int {
	low := strings.ToLower(tag)
	if lowerRef == "latest" || lowerRef == "" {
		switch {
		case semverTagRe.MatchString(tag):
			return 0
		case shaLikeTagRe.MatchString(tag):
			return 1
		default:
			return 2
		}
	}

	switch {
	case low == lowerRef:
		return 0
	case shaLikeTagRe.MatchString(tag):
		return 1
	case strings.Contains(low, lowerRef):
		return 2
	case semverTagRe.MatchString(tag):
		return 3
	default:
		return 4
	}
}

func parseAuthChallenge(h, repo string) (realm, service, scope string, err error) {
	// Example: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(h)), "bearer ") {
		return "", "", "", fmt.Errorf("unsupported auth challenge: %s", h)
	}
	params := map[string]string{}
	rest := strings.TrimSpace(h[len("Bearer "):])
	for _, p := range splitCSVRespectQuotes(rest) {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		val := strings.Trim(kv[1], "\"")
		params[key] = val
	}
	realm = params["realm"]
	service = params["service"]
	scope = params["scope"]
	if realm == "" {
		return "", "", "", fmt.Errorf("missing realm in auth challenge")
	}
	// If scope wasn't provided, default to repository:repo:pull
	if scope == "" && repo != "" {
		scope = fmt.Sprintf("repository:%s:pull", repo)
	}
	return
}

func splitCSVRespectQuotes(s string) []string {
	var out []string
	var cur strings.Builder
	inQ := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch ch {
		case '"':
			inQ = !inQ
			cur.WriteByte(ch)
		case ',':
			if inQ {
				cur.WriteByte(ch)
			} else {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func fetchToken(ctx context.Context, realm, service, scope, user, pass string) (string, error) {
	u, err := url.Parse(realm)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if service != "" {
		q.Set("service", service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	u.RawQuery = q.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed: %s: %s", resp.Status, string(body))
	}
	var out struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	tok := out.Token
	if tok == "" {
		tok = out.AccessToken
	}
	if tok == "" {
		return "", fmt.Errorf("no token in response")
	}
	return tok, nil
}

func (c *Client) logInfo(format string, a ...any) {
	if c.logger != nil {
		c.logger.Info(fmt.Sprintf(format, a...))
	}
}

func (c *Client) logDebug(format string, a ...any) {
	if c.logger != nil {
		c.logger.Debug(fmt.Sprintf(format, a...))
	}
}
