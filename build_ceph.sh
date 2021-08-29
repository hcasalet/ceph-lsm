#!/bin/bash -x

# To build: 1, % id (-- to find the uid:gid)
# 2, sudo su
# 3, chown -R uid:gid /mydata (--to grant the extra disk to myself)
# 4, exit; cd /mydata  (--back to myself)
# 5, git clone https://github.com/hcasalet/ceph-lsm.git
# 6, cd ceph-lsm
# 7, git submodule update --init --recursive
# 8, sudo su (--be the root for the rest)

apt-get update

./install-deps.sh

apt-get install libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libgconf-2-4 libnss3 libxss1 libasound2 libxtst6 xauth xvfb -y

apt install nodejs -y

apt install npm -y

./do_cmake.sh -DCMAKE_BUILD_TYPE=RelWithDebInfo

chmod -R 777 ~/.cache

chmod -R 777 build/src/pybind/mgr/dashboard

cd build

ninja