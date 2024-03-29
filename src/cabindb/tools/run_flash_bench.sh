#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: benchmark.sh exists in the current directory
# After execution of this script, log files are generated in $output_dir.
# report.txt provides a high level statistics

# This should be run from the parent of the tools directory. The command line is:
#   [$env_vars] tools/run_flash_bench.sh [list-of-threads]
#
# This runs a sequence of tests in the following sequence:
#   step 1) load - bulkload, compact, fillseq, overwrite
#   step 2) read-only for each number of threads
#   step 3) read-write for each number of threads
#   step 4) merge for each number of threads
#
# The list of threads is optional and when not set is equivalent to "24". 
# Were list-of-threads specified as "1 2 4" then the tests in steps 2, 3 and
# 4 above would be repeated for 1, 2 and 4 threads. The tests in step 1 are
# only run for 1 thread.

# Test output is written to $OUTPUT_DIR, currently /tmp/output. The performance
# summary is in $OUTPUT_DIR/report.txt. There is one file in $OUTPUT_DIR per
# test and the tests are listed below.
#
# The environment variables are also optional. The variables are:
#
#   NKEYS         - number of key/value pairs to load
#   BG_MBWRITEPERSEC - write rate limit in MB/second for tests in which
#                   there is one thread doing writes and stats are
#                   reported for read threads. "BG" stands for background.
#                   If this is too large then the non-writer threads can get
#                   starved. This is used for the "readwhile" tests.
#   FG_MBWRITEPERSEC - write rate limit in MB/second for tests like overwrite
#                   where stats are reported for the write threads.
#   NSECONDS      - number of seconds for which to run each test in steps 2,
#                   3 and 4. There are currently 15 tests in those steps and
#                   they are repeated for each entry in list-of-threads so
#                   this variable lets you control the total duration to
#                   finish the benchmark.
#   RANGE_LIMIT   - the number of rows to read per range query for tests that
#                   do range queries.
#   VAL_SIZE      - the length of the value in the key/value pairs loaded.
#                   You can estimate the size of the test database from this,
#                   NKEYS and the compression rate (--compression_ratio) set
#                   in tools/benchmark.sh
#   BLOCK_LENGTH  - value for db_bench --block_size
#   CACHE_BYTES   - the size of the CabinDB block cache in bytes
#   DATA_DIR      - directory in which to create database files
#   LOG_DIR       - directory in which to create WAL files, may be the same
#                   as DATA_DIR
#   DO_SETUP      - when set to 0 then a backup of the database is copied from
#                   $DATA_DIR.bak to $DATA_DIR and the load tests from step 1
#                   The WAL directory is also copied from a backup if
#                   DATA_DIR != LOG_DIR. This allows tests from steps 2, 3, 4
#                   to be repeated faster.
#   SAVE_SETUP    - saves a copy of the database at the end of step 1 to
#                   $DATA_DIR.bak. When LOG_DIR != DATA_DIR then it is copied
#                   to $LOG_DIR.bak.
#   SKIP_LOW_PRI_TESTS - skip some of the tests which aren't crucial for getting
#                   actionable benchmarking data (look for keywords "bulkload",
#                   "sync=1", and "while merging").
#

# Size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

num_keys=${NKEYS:-$((1 * G))}
# write rate for readwhile... tests
bg_mbwps=${BG_MBWRITEPERSEC:-4}
# write rate for tests other than readwhile, 0 means no limit
fg_mbwps=${FG_MBWRITEPERSEC:-0}
duration=${NSECONDS:-$((60 * 60))}
nps=${RANGE_LIMIT:-10}
vs=${VAL_SIZE:-400}
cs=${CACHE_BYTES:-$(( 1 * G ))}
bs=${BLOCK_LENGTH:-8192}

# If no command line arguments then run for 24 threads.
if [[ $# -eq 0 ]]; then
  nthreads=( 24 )
else
  nthreads=( "$@" )
fi

for num_thr in "${nthreads[@]}" ; do
  echo Will run for $num_thr threads
done

# Update these parameters before execution !!!
db_dir=${DATA_DIR:-"/tmp/cabindb/"}
wal_dir=${LOG_DIR:-"/tmp/cabindb/"}

do_setup=${DO_SETUP:-1}
save_setup=${SAVE_SETUP:-0}

# By default we'll run all the tests. Set this to skip a set of tests which
# aren't critical for getting key metrics.
skip_low_pri_tests=${SKIP_LOW_PRI_TESTS:-0}

if [[ $skip_low_pri_tests == 1 ]]; then
  echo "Skipping some non-critical tests because SKIP_LOW_PRI_TESTS is set."
fi

output_dir="${TMPDIR:-/tmp}/output"

ARGS="\
OUTPUT_DIR=$output_dir \
NUM_KEYS=$num_keys \
DB_DIR=$db_dir \
WAL_DIR=$wal_dir \
VALUE_SIZE=$vs \
BLOCK_SIZE=$bs \
CACHE_SIZE=$cs"

mkdir -p $output_dir
echo -e "ops/sec\tmb/sec\tSize-GB\tL0_GB\tSum_GB\tW-Amp\tW-MB/s\tusec/op\tp50\tp75\tp99\tp99.9\tp99.99\tUptime\tStall-time\tStall%\tTest" \
  > $output_dir/report.txt

# Notes on test sequence:
#   step 1) Setup database via sequential fill followed by overwrite to fragment it.
#           Done without setting DURATION to make sure that overwrite does $num_keys writes
#   step 2) read-only tests for all levels of concurrency requested
#   step 3) non read-only tests for all levels of concurrency requested
#   step 4) merge tests for all levels of concurrency requested. These must come last.

###### Setup the database

if [[ $do_setup != 0 ]]; then
  echo Doing setup

  if [[ $skip_low_pri_tests != 1 ]]; then
    # Test 1: bulk load
    env $ARGS ./tools/benchmark.sh bulkload
  fi

  # Test 2a: sequential fill with large values to get peak ingest
  #          adjust NUM_KEYS given the use of larger values
  env $ARGS BLOCK_SIZE=$((1 * M)) VALUE_SIZE=$((32 * K)) NUM_KEYS=$(( num_keys / 64 )) \
       ./tools/benchmark.sh fillseq_disable_wal

  # Test 2b: sequential fill with the configured value size
  env $ARGS ./tools/benchmark.sh fillseq_disable_wal

  # Test 2c: same as 2a, but with WAL being enabled.
  env $ARGS BLOCK_SIZE=$((1 * M)) VALUE_SIZE=$((32 * K)) NUM_KEYS=$(( num_keys / 64 )) \
       ./tools/benchmark.sh fillseq_enable_wal

  # Test 2d: same as 2b, but with WAL being enabled.
  env $ARGS ./tools/benchmark.sh fillseq_enable_wal

  # Test 3: single-threaded overwrite
  env $ARGS NUM_THREADS=1 DB_BENCH_NO_SYNC=1 ./tools/benchmark.sh overwrite

else
  echo Restoring from backup

  rm -rf $db_dir

  if [ ! -d ${db_dir}.bak ]; then
    echo Database backup does not exist at ${db_dir}.bak
    exit -1
  fi

  echo Restore database from ${db_dir}.bak
  cp -p -r ${db_dir}.bak $db_dir

  if [[ $db_dir != $wal_dir ]]; then
    rm -rf $wal_dir

    if [ ! -d ${wal_dir}.bak ]; then
      echo WAL backup does not exist at ${wal_dir}.bak
      exit -1
    fi

    echo Restore WAL from ${wal_dir}.bak
    cp -p -r ${wal_dir}.bak $wal_dir
  fi
fi

if [[ $save_setup != 0 ]]; then
  echo Save database to ${db_dir}.bak
  cp -p -r $db_dir ${db_dir}.bak

  if [[ $db_dir != $wal_dir ]]; then
    echo Save WAL to ${wal_dir}.bak
    cp -p -r $wal_dir ${wal_dir}.bak
  fi
fi

###### Read-only tests

for num_thr in "${nthreads[@]}" ; do
  # Test 4: random read
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr ./tools/benchmark.sh readrandom

  # Test 5: random range scans
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr NUM_NEXTS_PER_SEEK=$nps \
    ./tools/benchmark.sh fwdrange

  # Test 6: random reverse range scans
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr NUM_NEXTS_PER_SEEK=$nps \
    ./tools/benchmark.sh revrange
done

###### Non read-only tests

for num_thr in "${nthreads[@]}" ; do
  # Test 7: overwrite with sync=0
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$fg_mbwps \
    DB_BENCH_NO_SYNC=1 ./tools/benchmark.sh overwrite

  if [[ $skip_low_pri_tests != 1 ]]; then
    # Test 8: overwrite with sync=1
    env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$fg_mbwps \
      ./tools/benchmark.sh overwrite
  fi

  # Test 9: random update with sync=0
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr DB_BENCH_NO_SYNC=1 \
      ./tools/benchmark.sh updaterandom

  if [[ $skip_low_pri_tests != 1 ]]; then
    # Test 10: random update with sync=1
   env $ARGS DURATION=$duration NUM_THREADS=$num_thr ./tools/benchmark.sh updaterandom
  fi

  # Test 11: random read while writing
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
    DB_BENCH_NO_SYNC=1 ./tools/benchmark.sh readwhilewriting

  # Test 12: range scan while writing
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
    DB_BENCH_NO_SYNC=1 NUM_NEXTS_PER_SEEK=$nps ./tools/benchmark.sh fwdrangewhilewriting

  # Test 13: reverse range scan while writing
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
    DB_BENCH_NO_SYNC=1 NUM_NEXTS_PER_SEEK=$nps ./tools/benchmark.sh revrangewhilewriting
done

###### Merge tests

for num_thr in "${nthreads[@]}" ; do
  # Test 14: random merge with sync=0
  env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$fg_mbwps \
    DB_BENCH_NO_SYNC=1 ./tools/benchmark.sh mergerandom

  if [[ $skip_low_pri_tests != 1 ]]; then
    # Test 15: random merge with sync=1
    env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$fg_mbwps \
      ./tools/benchmark.sh mergerandom

    # Test 16: random read while merging 
    env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
      DB_BENCH_NO_SYNC=1 ./tools/benchmark.sh readwhilemerging

    # Test 17: range scan while merging 
    env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
      DB_BENCH_NO_SYNC=1 NUM_NEXTS_PER_SEEK=$nps ./tools/benchmark.sh fwdrangewhilemerging

    # Test 18: reverse range scan while merging 
    env $ARGS DURATION=$duration NUM_THREADS=$num_thr MB_WRITE_PER_SEC=$bg_mbwps \
      DB_BENCH_NO_SYNC=1 NUM_NEXTS_PER_SEEK=$nps ./tools/benchmark.sh revrangewhilemerging
  fi
done

###### Universal compaction tests.

# Use a single thread to reduce the variability in the benchmark.
env $ARGS COMPACTION_TEST=1 NUM_THREADS=1 ./tools/benchmark.sh universal_compaction

if [[ $skip_low_pri_tests != 1 ]]; then
  echo bulkload > $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep bulkload $output_dir/report.txt >> $output_dir/report2.txt
fi

echo fillseq_wal_disabled >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fillseq.wal_disabled $output_dir/report.txt >> $output_dir/report2.txt

echo fillseq_wal_enabled >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fillseq.wal_enabled $output_dir/report.txt >> $output_dir/report2.txt

echo overwrite sync=0 >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep overwrite $output_dir/report.txt | grep \.s0  >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo overwrite sync=1 >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep overwrite $output_dir/report.txt | grep \.s1  >> $output_dir/report2.txt
fi

echo updaterandom sync=0 >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep updaterandom $output_dir/report.txt | grep \.s0 >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo updaterandom sync=1 >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep updaterandom $output_dir/report.txt | grep \.s1 >> $output_dir/report2.txt
fi

echo mergerandom sync=0 >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep mergerandom $output_dir/report.txt | grep \.s0 >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo mergerandom sync=1 >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep mergerandom $output_dir/report.txt | grep \.s1 >> $output_dir/report2.txt
fi

echo readrandom >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep readrandom $output_dir/report.txt  >> $output_dir/report2.txt

echo fwdrange >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fwdrange\.t $output_dir/report.txt >> $output_dir/report2.txt

echo revrange >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep revrange\.t $output_dir/report.txt >> $output_dir/report2.txt

echo readwhile >> $output_dir/report2.txt >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep readwhilewriting $output_dir/report.txt >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo readwhile >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep readwhilemerging $output_dir/report.txt >> $output_dir/report2.txt
fi

echo fwdreadwhilewriting >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep fwdrangewhilewriting $output_dir/report.txt >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo fwdreadwhilemerging >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep fwdrangewhilemerg $output_dir/report.txt >> $output_dir/report2.txt
fi

echo revreadwhilewriting >> $output_dir/report2.txt
head -1 $output_dir/report.txt >> $output_dir/report2.txt
grep revrangewhilewriting $output_dir/report.txt >> $output_dir/report2.txt

if [[ $skip_low_pri_tests != 1 ]]; then
  echo revreadwhilemerging >> $output_dir/report2.txt
  head -1 $output_dir/report.txt >> $output_dir/report2.txt
  grep revrangewhilemerg $output_dir/report.txt >> $output_dir/report2.txt
fi

cat $output_dir/report2.txt
