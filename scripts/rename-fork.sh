#!/usr/bin/env bash
#
# rename-fork.sh — rewrite the module namespace from the upstream cloudflare
# path to this fork's jpillora path, so the fork can be imported directly as
# github.com/jpillora/cloudflared/... (see FORK.md).
#
# This is a MECHANICAL, IDEMPOTENT transform. The upstream-tracking work
# (master == cloudflare/cloudflared) stays on the original import paths; run
# this after syncing upstream to (re)apply the fork namespace. Running it twice
# is a no-op.
#
# It rewrites Go import paths, `-X` ldflag symbol paths, tooling prefixes
# (goimports -local, go vet, IMPORT_PATH) and capnp $Go.import annotations.
# It deliberately LEAVES http(s):// URLs (docs, OCI image labels) pointing at
# the upstream project — those are protected by the //-lookbehind below.
#
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

OLD='github.com/cloudflare/cloudflared'
NEW='github.com/jpillora/cloudflared'

command -v perl >/dev/null || { echo "rename-fork.sh: perl is required" >&2; exit 1; }

# Files to transform: tracked Go sources (excluding vendored deps) plus the
# specific build/config files that embed the import path. Docs (*.md) and
# vendor/ are intentionally excluded.
mapfile -t FILES < <(
  {
    git ls-files '*.go' | grep -v '^vendor/'
    git ls-files 'go.mod' 'Makefile' 'Dockerfile*' '*.capnp'
    git ls-files '*.sh' | grep -E '^\.ci/'
  } | sort -u
)

changed=0
for f in "${FILES[@]}"; do
  [ -f "$f" ] || continue
  before=$(git hash-object "$f")
  # Rewrite OLD -> NEW unless immediately preceded by "//" (i.e. part of a URL).
  perl -i -pe 's{(?<!//)\Qgithub.com/cloudflare/cloudflared\E}{github.com/jpillora/cloudflared}g' "$f"
  after=$(git hash-object "$f")
  [ "$before" != "$after" ] && changed=$((changed + 1)) || true
done

echo "rename-fork.sh: $OLD -> $NEW"
echo "  scanned ${#FILES[@]} files, modified ${changed}."
