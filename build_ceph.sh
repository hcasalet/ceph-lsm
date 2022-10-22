#!/bin/bash -x

# some notes about using CloudLab: 
# url: cloudlab.us
# profile: small-lan
# physical node type: m510
# Temporary Filesystem Size: 100GB

# To build: 1, % id (-- to find the uid:gid)
# 2, sudo su
# 3, chown -R uid:gid /mydata (--to grant the extra disk to myself)
# 4, exit; cd /mydata  (--back to myself)
# 5, git clone https://github.com/hcasalet/ceph-lsm.git
# 6, cd ceph-lsm
# 7, git submodule update --init --recursive
# 8, sudo su (--be the root for the rest)

# there are times when error like "certification verification failed, the certificate is NOT trusted" is seen
# this could be caused by corrupted cache, and can be resolved by: 
# sudo apt clean; sudo apt update; sudo apt upgrade
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

# To run Ceph 
../src/stop.sh 
rm -rf out dev
ulimit -l unlimited
MON=1 OSD=3 MDS=0 MGR=1 FS=0 RGW=0 NFS=0 ../src/vstart.sh -d -n -x -i 127.0.0.1 --without-dashboard

# create pool
cd build
bin/ceph osd pool create cephlsm

# list pools
bin/ceph osd lspools

# build lsm
bin/ceph_test_cls_lsm

# Check the object
bin/rados -p <pool_name> ls

# Run YCSB
./bin/ycsb_cephlsm -db cephlsm -P "../src/tools/ycsb/workloads/test_workloada.spec" -threads 1 -load true -run true

./bin/ycsb_cephlsm -db leveldb -P "../src/tools/ycsb/workloads/test_workloada.spec" -threads 1 -load true -run true