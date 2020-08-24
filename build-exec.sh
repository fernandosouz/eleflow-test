#!/usr/bin/env bash

home="$(
  cd "$(dirname "$0")"
  pwd -P
)"

echo ">>> Building application..."
mvn clean install --quiet

./exec.sh $1