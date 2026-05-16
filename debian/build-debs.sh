#!/usr/bin/env bash
# Build all six MMORPG .deb packages.
#
# Inputs (env):
#   VERSION         - semver string, e.g. "0.1.0"
#   IMAGE_NAMESPACE - GHCR namespace (e.g. "lordnns")
#   HOST_AGENT_BIN  - path to pre-built host_agent binary
#
# Outputs: dist/*.deb

set -euo pipefail

: "${VERSION:?VERSION env var required}"
: "${IMAGE_NAMESPACE:?IMAGE_NAMESPACE env var required}"
: "${HOST_AGENT_BIN:?HOST_AGENT_BIN env var required}"

if [ ! -f "$HOST_AGENT_BIN" ]; then
    echo "ERROR: host_agent binary not found at $HOST_AGENT_BIN" >&2
    exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
mkdir -p "$DIST"

REGISTRY="ghcr.io"
COMPOSE_PACKAGES=(monolith central-fleet redis gatekeeper orchestrator)

# Replace __VERSION__ / __REGISTRY__ / __NAMESPACE__ placeholders.
substitute_placeholders() {
    local staging="$1"
    find "$staging" -type f \
        \( -name "*.yaml" -o -name "control" -o -name "*.service" \
           -o -name "postinst" -o -name "prerm" -o -name "postrm" \
           -o -path "*/etc/default/*" \) \
        -print0 | while IFS= read -r -d '' f; do
        sed -i \
            -e "s|__VERSION__|$VERSION|g" \
            -e "s|__REGISTRY__|$REGISTRY|g" \
            -e "s|__NAMESPACE__|$IMAGE_NAMESPACE|g" \
            "$f"
    done
}

build_compose_deb() {
    local pkg="$1"
    local staging="/tmp/build-mmorpg-$pkg"
    rm -rf "$staging"

    echo "=== Building mmorpg-$pkg ==="

    cp -r "$ROOT/debian/$pkg" "$staging"
    substitute_placeholders "$staging"

    chmod 0755 "$staging/DEBIAN/postinst" 2>/dev/null || true
    chmod 0755 "$staging/DEBIAN/prerm"    2>/dev/null || true
    chmod 0755 "$staging/DEBIAN/postrm"   2>/dev/null || true

    fakeroot dpkg-deb --build --root-owner-group "$staging" \
        "$DIST/mmorpg-${pkg}_${VERSION}_all.deb"
}

build_fleet_ds_deb() {
    local pkg="fleet-ds"
    local staging="/tmp/build-mmorpg-$pkg"
    rm -rf "$staging"

    echo "=== Building mmorpg-$pkg ==="

    cp -r "$ROOT/debian/$pkg" "$staging"

    mkdir -p "$staging/usr/local/bin"
    cp "$HOST_AGENT_BIN" "$staging/usr/local/bin/host_agent"
    chmod 0755 "$staging/usr/local/bin/host_agent"

    substitute_placeholders "$staging"

    chmod 0755 "$staging/DEBIAN/postinst" 2>/dev/null || true
    chmod 0755 "$staging/DEBIAN/prerm"    2>/dev/null || true
    chmod 0755 "$staging/DEBIAN/postrm"   2>/dev/null || true

    fakeroot dpkg-deb --build --root-owner-group "$staging" \
        "$DIST/mmorpg-${pkg}_${VERSION}_amd64.deb"
}

for pkg in "${COMPOSE_PACKAGES[@]}"; do
    build_compose_deb "$pkg"
done

build_fleet_ds_deb

echo
echo "=== Built packages ==="
ls -lh "$DIST"/*.deb
