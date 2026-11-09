#!/usr/bin/env bash
# DEP-3: pins the Node base image in backend/Dockerfile.sender by digest.
# Usage: backend/scripts/pin-node-digest.sh [tag]      (default tag: 22-slim)
# Needs Docker (buildx) or curl with access to registry-1.docker.io. Review the diff and commit it.
set -euo pipefail

tag="${1:-22-slim}"
file="$(cd "$(dirname "$0")/.." && pwd)/Dockerfile.sender"

digest=""
if command -v docker >/dev/null 2>&1; then
  # The digest of the multi-arch index: the Dockerfile's --platform picks the arm64 image from it.
  digest="$(docker buildx imagetools inspect "node:${tag}" --format '{{.Manifest.Digest}}' 2>/dev/null || true)"
fi
if [[ -z "$digest" ]]; then
  token="$(curl -fsS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/node:pull" | sed -E 's/.*"token":"([^"]+)".*/\1/')"
  digest="$(curl -fsSI \
    -H "Authorization: Bearer ${token}" \
    -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json' \
    "https://registry-1.docker.io/v2/library/node/manifests/${tag}" \
    | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')"
fi

if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "could not resolve a digest for node:${tag} (got: '${digest}')" >&2
  exit 1
fi

sed -i.bak -E "s#^(FROM --platform=linux/arm64 )node:[^ @]+(@sha256:[0-9a-f]+)?#\1node:${tag}@${digest}#" "$file"
sed -i.bak -E '/^# TODO\(DEP-3\)/,/^# digest-pinned nor covered/d' "$file"
rm -f "${file}.bak"
echo "pinned node:${tag} to ${digest}"
