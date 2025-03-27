#!/bin/bash

platforms=("linux/amd64" "linux/arm64")

version=$(grep -E 'VERSION = "[^"]+"' src/config.go | sed -E 's/.*VERSION = "([^"]+)".*/\1/')
if [ -z "$version" ]; then
  echo "Error: Could not extract version from config.go"
  exit 1
fi

for platform in "${platforms[@]}"
do
  os="${platform%/*}"
  arch="${platform#*/}"
  tag="ghcr.io/bemihq/bemidb:$version-$arch"

  echo "Building bemidb version $version for $os/$arch"

  docker buildx build \
    --build-arg PLATFORM=$platform \
    --build-arg GOOS=$os \
    --build-arg GOARCH="$arch" \
    -t $tag .

  docker create --name temp-container $tag
  docker cp temp-container:/app/bemidb ./build/bemidb-$os-$arch
  docker rm temp-container
done
