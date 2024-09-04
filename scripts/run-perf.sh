#!/bin/bash
#
# Copyright (c) 2024 Contributors to the Eclipse Foundation
#
# Building all currently supported targets for databroker-cli.
# Uses cross for cross-compiling. Needs to be executed
# before docker build, as docker collects the artifacts
# created by this script
# this needs the have cross, cargo-license and kuksa sbom helper
# installed
#
# SPDX-License-Identifier: Apache-2.0

# To be run from root of kuksa-databroker-perf

if [ $# -ne 0 ]; then
    echo "Usage (from repo root): run-perf.sh"
    exit 1
fi


echo "Building perftest"
cargo build --release
echo "Starting Databroker (latest main)"
databroker_id=$(docker run -d -it --rm -p 55555:55555 ghcr.io/eclipse-kuksa/kuksa-databroker:main --insecure --enable-databroker-v1)
echo "Started as $databroker_id, now wait 5 seconds to assure it is up"
sleep 5
pwd
for filename in configs/*.json;
do
  echo "************ Running perftest for $filename ************************"
  time ./target/release/databroker-perf --config $filename
  echo "************ Perftest finished for $filename ************************"
done
echo Shutting down Databroker
docker stop $databroker_id
