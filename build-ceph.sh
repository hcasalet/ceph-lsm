#!/bin/bash

cd ceph-lsm

git submodule update --init --recursive

apt-get update

./install-deps.sh

./do_cmake.sh -DCMAKE_BUILD_TYPE=RelWithDebInfo

apt-get install libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libgconf-2-4 libnss3 libxss1 libasound2 libxtst6 xauth xvfb -y

apt-get install nodejs -y

apt-get install npm -y

chmod -R 777 ~/.cache

chmod -R 777 build/src/pybind/mgr/dashboard

ninja