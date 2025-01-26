#!/bin/bash

# Check if a commit message is provided
if [ -z "$1" ]; then
  echo "Error: No commit message provided."
  exit 1
fi

# Stage all changes
git add .

# Commit with the provided message
git commit . -m "AlexisLeclerc01212025"

# Push to the remote repository (optional)
git push origin HEAD

git push origin HEAD:master
