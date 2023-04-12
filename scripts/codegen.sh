#!/bin/sh

docker build -t pg-schema-diff-code-gen-runner -f ./build/Dockerfile.codegen .
docker run --rm -v $(pwd):/pg-schema-diff -w /pg-schema-diff  pg-schema-diff-code-gen-runner
