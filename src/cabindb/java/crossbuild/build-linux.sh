#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# install all required packages for cabindb
sudo apt-get update
sudo apt-get -y install git make gcc g++ libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev default-jdk

# set java home so we can build cabindb jars
export JAVA_HOME=$(echo /usr/lib/jvm/java-7-openjdk*)
cd /cabindb
make jclean clean
make -j 4 cabindbjavastatic
cp /cabindb/java/target/libcabindbjni-* /cabindb-build
cp /cabindb/java/target/cabindbjni-* /cabindb-build
sudo shutdown -h now

