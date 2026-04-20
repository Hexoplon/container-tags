Container Tags Resolver (Go)

CLI that resolves what concrete release tag (e.g., v1.2.3) a given tag (default: latest) actually points to on OCI/Docker registries by matching manifest digests.

Supports public images on docker.io (Docker Hub) and ghcr.io; should work with most Registry v2 endpoints that provide token auth. No external dependencies.

Usage

- Build: `go build ./cmd/container-tags`
- Run: `./container-tags [-v|-vv] [-i|--infer-only] [--tag-only|-t] [-u USER -p PASS | --registry-token TOKEN] <image-ref>`

Examples

- `./container-tags ghcr.io/owner/app`
- `./container-tags docker.io/library/nginx:latest`
- `./container-tags nginx` (defaults to `docker.io/library/nginx`)
- `./container-tags -v nginx` (verbose logging)
- `./container-tags -vv nginx` (very verbose; includes per-tag checks)
- `./container-tags -i nginx` (read version labels from latest; no tag scan)
- `./container-tags --infer-only nginx` (same as -i)
- `./container-tags -t nginx` (print only resolved tag/version)
- `./container-tags -u $GHCR_USERNAME -p $GHCR_TOKEN ghcr.io/owner/app` (authenticate to GHCR)
- `./container-tags -u $DOCKERHUB_USERNAME -p $DOCKERHUB_PASSWORD docker.io/library/nginx` (authenticate to Docker Hub)

Behavior

- Parses the image reference and defaults to `docker.io` and `library/` when omitted.
- Tries a fast path first: GETs the manifest (and, if needed, image config blob) for the given tag and reads common version metadata labels (e.g., `org.opencontainers.image.version`). If found, returns immediately without scanning tags.
- If no version labels are found, fetches the digest for the provided tag (default `latest`) and scans other tags to find the matching digest. It prioritizes likely matches based on the input tag, runs manifest lookups concurrently, and caches digests both in memory and on disk for later runs.
- Prints a simple mapping line: `<image>:<input-tag> -> <matched-tag> (digest <sha256:...>)`.

Authentication

- Flags:
  - `-u, --username` and `-p, --password` supply basic credentials for the registry (use a PAT as the password for GHCR/Docker Hub).
  - `--registry-token` supplies an explicit registry bearer token (advanced).
- Environment (auto-detected when flags are omitted):
  - GHCR: `GHCR_USERNAME`, `GHCR_TOKEN` (or `CR_PAT`). Optional: `GHCR_REGISTRY_TOKEN`.
  - Docker Hub: `DOCKERHUB_USERNAME` or `DOCKER_USERNAME`; `DOCKERHUB_PASSWORD` or `DOCKER_PASSWORD`. Optional: `DOCKERHUB_REGISTRY_TOKEN`.
  - Generic: `REGISTRY_BEARER_TOKEN`, `REGISTRY_PASSWORD`.

Notes

- Only public images are supported out-of-the-box. For private registries, extend the client to accept credentials and pass them to the token service.
- Network errors or registry-specific quirks may affect results; the tool falls back from HEAD to GET to retrieve digest headers when necessary.
- Digest cache is stored at `$XDG_CACHE_HOME/container-tags/digest-cache.json` when available. Override with `CONTAINER_TAGS_CACHE_PATH`.
