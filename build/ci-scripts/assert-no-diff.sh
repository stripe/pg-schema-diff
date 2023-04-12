#!/bin/bash

git_status=$(git status --porcelain)
if [[ -n $git_status ]]; then
  echo "Changes to generated files detected $git_status"
  exit 1
fi
