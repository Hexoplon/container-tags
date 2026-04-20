package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"container-tags/internal/refs"
	"container-tags/internal/registry"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] <image-ref>\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "\nResolves what concrete tag (e.g., v1.2.3) the given tag (default: latest) actually points to by matching manifest digests or by inferring from labels.")
	fmt.Fprintln(os.Stderr, "\nFlags:")
	pflag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nExamples:")
	fmt.Fprintln(os.Stderr, "  container-tags ghcr.io/owner/app")
	fmt.Fprintln(os.Stderr, "  container-tags docker.io/library/nginx:latest")
	fmt.Fprintln(os.Stderr, "  container-tags -v nginx  # resolves on docker.io")
}

func main() {
	var v bool
	var vv bool
	var inferOnly bool
	var tagOnly bool
	var username string
	var password string
	var registryToken string
	pflag.BoolVarP(&v, "verbose", "v", false, "verbose logging")
	pflag.BoolVar(&vv, "vv", false, "very verbose (debug) logging")
	pflag.BoolVarP(&inferOnly, "infer-only", "i", false, "only infer version from labels; do not scan tags")
	pflag.BoolVarP(&tagOnly, "tag-only", "t", false, "print only the resolved tag/version to stdout")
	pflag.StringVarP(&username, "username", "u", "", "registry username (GHCR or Docker Hub)")
	pflag.StringVarP(&password, "password", "p", "", "registry password / token (PAT for GHCR or Docker Hub)")
	pflag.StringVar(&registryToken, "registry-token", "", "explicit registry bearer token (advanced)")
	pflag.Usage = usage
	pflag.Parse()
	if pflag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	imageStr := pflag.Arg(0)
	img, err := refs.Parse(imageStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	client := registry.NewClient()
	if vv {
		client.SetVerbosity(2)
	} else if v {
		client.SetVerbosity(1)
	}

	tag := img.Tag
	if tag == "" {
		tag = "latest"
	}

	ensureAuthForRegistry(client, img.Registry, &username, &password, &registryToken)
	if registryToken != "" {
		client.SetStaticBearer(registryToken)
	}
	if username != "" {
		client.SetBasicAuth(username, password)
	}

	if inferOnly {
		d, v, ok, err := client.InferVersionFromTag(img, tag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error inferring version: %v\n", err)
			client.Close()
			os.Exit(1)
		}
		if !ok {
			fmt.Fprintf(os.Stderr, "no version labels found on %s:%s\n", refs.String(img), tag)
			client.Close()
			os.Exit(4)
		}
		if tagOnly {
			fmt.Printf("%s\n", v)
		} else {
			fmt.Printf("%s:%s -> %s (digest %s)\n", refs.String(img), tag, v, d)
		}
		client.Close()
		return
	}

	latestDigest, matched, err := client.ResolveTagAlias(img, tag)
	if err != nil {
		if errors.Is(err, registry.ErrNoMatchingTag) {
			fmt.Fprintf(os.Stderr, "no matching tag found for %s:%s (digest %s)\n", refs.String(img), tag, latestDigest)
			client.Close()
			os.Exit(3)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		client.Close()
		os.Exit(1)
	}

	if tagOnly {
		fmt.Printf("%s\n", matched)
	} else {
		fmt.Printf("%s:%s -> %s (digest %s)\n", refs.String(img), tag, matched, latestDigest)
	}
	client.Close()
}

// ensureAuthForRegistry tries to populate username/password/token from env if not provided via flags.
func ensureAuthForRegistry(client *registry.Client, reg string, user, pass, token *string) {
	if *token == "" {
		// Allow explicit bearer from env
		if t := os.Getenv("REGISTRY_BEARER_TOKEN"); t != "" {
			*token = t
		}
		// Registry-specific bearer
		switch reg {
		case "ghcr.io":
			if t := os.Getenv("GHCR_REGISTRY_TOKEN"); t != "" {
				*token = t
			}
		case "docker.io":
			if t := os.Getenv("DOCKERHUB_REGISTRY_TOKEN"); t != "" {
				*token = t
			}
		}
	}
	if *user == "" {
		switch reg {
		case "ghcr.io":
			if v := os.Getenv("GHCR_USERNAME"); v != "" {
				*user = v
			}
			if *pass == "" {
				if v := os.Getenv("GHCR_TOKEN"); v != "" {
					*pass = v
				}
				if v := os.Getenv("CR_PAT"); v != "" {
					*pass = v
				}
			}
		case "docker.io":
			if v := os.Getenv("DOCKERHUB_USERNAME"); v != "" {
				*user = v
			} else if v := os.Getenv("DOCKER_USERNAME"); v != "" {
				*user = v
			}
			if *pass == "" {
				if v := os.Getenv("DOCKERHUB_PASSWORD"); v != "" {
					*pass = v
				} else if v := os.Getenv("DOCKER_PASSWORD"); v != "" {
					*pass = v
				}
			}
		}
	} else if *pass == "" {
		// Username provided but password missing; try generic env
		if v := os.Getenv("REGISTRY_PASSWORD"); v != "" {
			*pass = v
		}
	}
}
