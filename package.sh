#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_NAME="${1:-profiles_ingest.zip}"

rm -rf build dist
mkdir -p build dist

if [ -f requirements.txt ] && [ -s requirements.txt ]; then
  pip install -r requirements.txt -t build/
fi

cp -R src/* build/

cd build
zip -r "../dist/${ARTIFACT_NAME}" .
cd ..

echo "Created dist/${ARTIFACT_NAME}"