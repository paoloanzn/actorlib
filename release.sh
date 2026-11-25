#!/bin/sh
# Cuts a release: tags main's HEAD and pushes main and the tag. Pushing the tag starts the
# Release workflow (.github/workflows/release.yml), which checks the tag against the version
# in CMakeLists.txt, runs CI on the tagged commit and publishes the GitHub release.
#
#   ./release.sh v0.1.0          tag and push
#   ./release.sh v0.1.0-alpha    the same, published as a pre-release
#   ./release.sh latest          print the newest tag
set -eu

RELEASE=${1:-}

if [ -z "$RELEASE" ]; then
    echo "usage: release [TAG | latest]"
    exit 1
fi

if [ "$RELEASE" = "latest" ]; then
    git fetch origin --tags --quiet
    LATEST=$(git tag --sort=-v:refname | head -n 1)
    if [ -z "$LATEST" ]; then
        echo "no tags found"
        exit 1
    fi
    echo "$LATEST"
    exit 0
fi

git tag -m "$RELEASE" "$RELEASE"
git push origin main "$RELEASE"
