#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -e
#set -x

# just in-case this is run outside Docker
mkdir -p /cabindb-local-build

rm -rf /cabindb-local-build/*
cp -r /cabindb-host/* /cabindb-local-build
cd /cabindb-local-build

make clean-not-downloaded
PORTABLE=1 make cabindbjavastatic

cp java/target/libcabindbjni-linux*.so java/target/cabindbjni-*-linux*.jar java/target/cabindbjni-*-linux*.jar.sha1x /cabindb-java-target

