#!/usr/bin/env bash

home="$(
  cd "$(dirname "$0")"
  pwd -P
)"

echo ">>> Executing application..."
spark-submit \
 --class "com.eleflow.br.LocalFuelAnalysisMain" \
 --master local[*] \
 --driver-java-options "-Dlog4j.configuration=file:$home/src/main/resources/log4j.properties" \
 $home/target/eleflow-test-1.0.0-jar-with-dependencies.jar $1
