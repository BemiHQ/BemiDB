#!/bin/bash

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Map architecture to Go naming convention
case $ARCH in
  x86_64|amd64)
    ARCH="amd64"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Set the download URL and binary name
BINARY_NAME="bemidb-${OS}-${ARCH}"
DOWNLOAD_URL="https://github.com/BemiHQ/BemiDB/releases/latest/download/$BINARY_NAME"

# Download the binary
echo "Downloading $DOWNLOAD_URL..."
curl -L "$DOWNLOAD_URL" -o ./bemidb

if [ "$ARCH" = "arm64" ] && [ "$OS" = "darwin" ]; then
  # Ensure /usr/local/lib exists
  if [ ! -d "/usr/local/lib" ]; then
    echo "Creating /usr/local/lib directory..."
    sudo mkdir -p /usr/local/lib
  fi

  # Download the libc++ dynamic libraries for macOS (can't be statically linked)
  curl -sL "https://github.com/BemiHQ/BemiDB/releases/latest/download/libc++.1.0.dylib" -o ./libc++.1.0.dylib
  sudo mv ./libc++.1.0.dylib /usr/local/lib/libc++.1.0.dylib
  curl -sL "https://github.com/BemiHQ/BemiDB/releases/latest/download/libc++abi.1.dylib" -o ./libc++abi.1.dylib
  sudo mv ./libc++abi.1.dylib /usr/local/lib/libc++abi.1.dylib
fi

# Make the binary executable
chmod +x ./bemidb
